use crate::order_book::{
    orderbook::OrderBook,
    types::{
        CancelOrder, CancelOutcome, GlobalOrderRegistry, ModifyOrder, ModifyOutcome, NewOrder, OrderLocation, OrderNode, OrderType
    },
};
use anyhow::Context;
use std::collections::HashMap;
use tracing::{Span, instrument};
use uuid::Uuid;

#[derive(Debug)]
pub struct MatchingEngine {
    _book: HashMap<Uuid, OrderBook>,
    _global_registry: GlobalOrderRegistry,
}

impl MatchingEngine {

    pub fn new() -> Self{
        Self { _book: HashMap::new(), _global_registry: GlobalOrderRegistry::new() }
    }

    #[instrument(
        name = "get_orderbook",
        skip(self),
        fields(
            order_id = %global_order_id
        )
    )]
    pub fn get_orderbook(
        &mut self,
        global_order_id: Uuid,
    ) -> Option<(usize, bool,Uuid, &mut OrderBook)> {
        let global_registry = GlobalOrderRegistry::new();
        let order_location = global_registry.get_details(&global_order_id)?;
        let Some(book) = self._book.get_mut(&order_location.security_id) else {
            return None;
        };
        Some((order_location.order_index, order_location.is_buy_side,order_location.security_id, book))
    }

    pub fn modify(
        &mut self,
        global_order_id: Uuid,
        new_price: Option<u32>,
        new_qty: Option<u32>,
        span: &Span,
    ) -> Result<(), anyhow::Error> {
        let (order_index, is_buy_side,security_id, orderbook) = self
            .get_orderbook(global_order_id)
            .context("Could not find the orderbook")?;
        if let Ok(potential_modfication) = orderbook.modify_order(
            global_order_id,
            ModifyOrder {
                new_price,
                order_index,
                is_buy_side,
                new_quantity: new_qty,
            },
        ) {
            if let Some(modification_result) = potential_modfication {
                match modification_result {
                    ModifyOutcome::Both {
                        new_price,
                        new_initial_qty,
                        old_current_qty,
                    } => {
                        span.record("modify_outcome", "price & qty");
                        if let Some(_) = self._global_registry.delete(&global_order_id){
                            let _ = self.match_order(
                            NewOrder {
                                engine_order_id: global_order_id,
                                price: new_price,
                                initial_quantity: new_initial_qty,
                                current_quantity : old_current_qty,
                                is_buy_side,
                                security_id,
                                order_type: OrderType::Limit,
                            },
                            span);
                            return Ok(());
                        }
                        span.record("intermediate_error", "Failed to delete from global registry");
                    },
                    ModifyOutcome::Repriced { new_price, old_initial_qty, old_current_qty } => 
                        {
                        span.record("modify_outcome", "price");
                        if let Some(_) = self._global_registry.delete(&global_order_id){
                            let _ = self.match_order(
                            NewOrder {
                                engine_order_id: global_order_id,
                                price: new_price,
                                initial_quantity: old_initial_qty,
                                current_quantity : old_current_qty,
                                is_buy_side,
                                security_id,
                                order_type: OrderType::Limit,
                            },
                            span);
                            return Ok(());
                        }
                        span.record("intermediate_error", "Failed to delete from global registry");
                    },
                    ModifyOutcome::Requantized { old_price, new_initial_qty, old_current_qty } => {
                        span.record("modify_outcome", "qty");
                        if let Some(_) = self._global_registry.delete(&global_order_id){
                            let _ = self.match_order(
                            NewOrder {
                                engine_order_id: global_order_id,
                                price: old_price,
                                initial_quantity: new_initial_qty,
                                current_quantity : old_current_qty,
                                is_buy_side,
                                security_id,
                                order_type: OrderType::Limit,
                            }, span);
                            return Ok(());
                        }
                        span.record("intermediate_error", "Failed to delete from global registry");
                    },
                    ModifyOutcome::Inplace => {
                        span.record("modify_outcome", "qty reduction");
                        return Ok(());
                    }
                }
            }
        } else {
            return Ok(());
        }
        Ok(())
    }

    pub fn cancel(&mut self, global_order_id: Uuid, span: &Span) -> Result<CancelOutcome, anyhow::Error>{
        let (order_index, is_buy_side,_, orderbook) = self
            .get_orderbook(global_order_id)
            .context("Could not find the orderbook")?;
        if let Err(_) = orderbook.cancel_order(global_order_id, CancelOrder{is_buy_side, order_index}){
            span.record("reason", "orderbook cancellation failed");
            span.record("success_status", false);
            return Ok(CancelOutcome::Failed);
        }; 
        if let Some(_) = self._global_registry.delete(&global_order_id){
            span.record("success_status", true);
            return Ok(CancelOutcome::Success)
        };
        span.record("reason", "Registry cancellation failed");
        span.record("success_status", false);
        Ok(CancelOutcome::Failed)
    }

    pub fn match_order(&mut self, order: NewOrder, span: &Span) -> Result<(), anyhow::Error> {
        let (_, _,_, orderbook) = self
            .get_orderbook(order.engine_order_id)
            .context("Could not find the orderbook")?; // context converts option into result. ? changes the Err to anyhow::error and add extra context to the error return by .context

        if !order.is_buy_side {
            // for ASK order
            match order.order_type {
                OrderType::Market(None) => {
                    // need to immediatly execute the order on the best of other half
                    let mut fill_quantity = order.initial_quantity;
                    let mut levels_touched = 0;
                    let mut orders_consumed = 0;
                    while fill_quantity > 0 {
                        let remove_node: bool;
                        {
                            let Some(mut price_node) = orderbook.bid.price_map.first_entry() else {
                                break;
                            };
                            let price_level = price_node.get_mut();
                            while price_level.total_quantity > 0 && fill_quantity > 0 {
                                let head_idx = price_level.head;
                                let first_order_node =
                                    orderbook.bid.order_pool[head_idx].as_mut().unwrap();
                                if fill_quantity >= first_order_node.current_quantity {
                                    fill_quantity -= first_order_node.current_quantity;
                                    price_level.total_quantity -= first_order_node.current_quantity;
                                    let next = first_order_node.next;
                                    orderbook.bid.order_pool[head_idx] = None;
                                    orderbook.bid.free_list.push(head_idx);
                                    orders_consumed += 1;
                                    if let Some(next_order_idx) = next {
                                        price_level.head = next_order_idx;
                                    } else {
                                        span.record("reason", "exhausted");
                                        break;
                                    }
                                } else {
                                    first_order_node.current_quantity -= fill_quantity;
                                    price_level.total_quantity -= fill_quantity;
                                    fill_quantity = 0;
                                    span.record("filled", true);
                                }
                            }
                            remove_node = price_level.total_quantity == 0;
                        }
                        if remove_node {
                            orderbook.bid.price_map.pop_first();
                            levels_touched += 1;
                        }
                    }
                    span.record("order_type", "market");
                    span.record("is_buy_side", false);
                    span.record("levels_touched", levels_touched);
                    span.record("order_consumed", orders_consumed);
                }
                OrderType::Market(market_limit) => {
                    let mut fill_quantity = order.initial_quantity;
                    let mut levels_touched = 0;
                    let mut orders_consumed = 0;
                    while fill_quantity > 0 {
                        let remove_node: bool;
                        {
                            let Some(mut price_node) = orderbook.bid.price_map.first_entry() else {
                                break;
                            };
                            if market_limit.unwrap() >= *price_node.key() {
                                break;
                            }
                            let price_level = price_node.get_mut();
                            while price_level.total_quantity > 0 && fill_quantity > 0 {
                                let head_idx = price_level.head;
                                let first_order_node =
                                    orderbook.bid.order_pool[head_idx].as_mut().unwrap();
                                if fill_quantity >= first_order_node.current_quantity {
                                    fill_quantity -= first_order_node.current_quantity;
                                    price_level.total_quantity -= first_order_node.current_quantity;
                                    let next = first_order_node.next;
                                    orderbook.bid.order_pool[head_idx] = None;
                                    orderbook.bid.free_list.push(head_idx);
                                    orders_consumed += 1;
                                    if let Some(next_order_idx) = next {
                                        price_level.head = next_order_idx;
                                    } else {
                                        span.record("reason", "exhausted");
                                        break;
                                    }
                                } else {
                                    first_order_node.current_quantity -= fill_quantity;
                                    price_level.total_quantity -= fill_quantity;
                                    fill_quantity = 0;
                                    span.record("filled", true);
                                }
                            }
                            remove_node = price_level.total_quantity == 0;
                        }
                        if remove_node {
                            orderbook.bid.price_map.pop_first();
                            levels_touched += 1;
                        }
                    }
                    span.record("order_type", "market");
                    span.record("is_buy_side", false);
                    span.record("levels_touched", levels_touched);
                    span.record("order_consumed", orders_consumed);
                }
                OrderType::Limit => {
                    let mut fill_quantity = order.initial_quantity;
                    let mut levels_touched = 0;
                    let mut orders_consumed = 0;
                    while fill_quantity > 0 {
                        let remove_node: bool;
                        {
                            let Some(mut price_node) = orderbook.bid.price_map.first_entry() else {
                                break;
                            };
                            if order.price >= *price_node.key() {
                                break;
                            }
                            let price_level = price_node.get_mut();
                            while price_level.total_quantity > 0 && fill_quantity > 0 {
                                let head_idx = price_level.head;
                                let first_order_node =
                                    orderbook.bid.order_pool[head_idx].as_mut().unwrap();
                                if fill_quantity >= first_order_node.current_quantity {
                                    fill_quantity -= first_order_node.current_quantity;
                                    price_level.total_quantity -= first_order_node.current_quantity;
                                    let next = first_order_node.next;
                                    orderbook.bid.order_pool[head_idx] = None;
                                    orderbook.bid.free_list.push(head_idx);
                                    orders_consumed += 1;
                                    if let Some(next_order_idx) = next {
                                        price_level.head = next_order_idx;
                                    } else {
                                        span.record("reason", "partially_filled");
                                        break;
                                    }
                                } else {
                                    first_order_node.current_quantity -= fill_quantity;
                                    price_level.total_quantity -= fill_quantity;
                                    fill_quantity = 0;
                                }
                            }
                            remove_node = price_level.total_quantity == 0;
                        }
                        if remove_node {
                            orderbook.bid.price_map.pop_first();
                            levels_touched += 1;
                        }
                    }
                    let alloted_index = orderbook.create_sell_order(
                        order.engine_order_id,
                        OrderNode {
                            initial_quantity: order.initial_quantity,
                            current_quantity: fill_quantity,
                            market_limit: order.price,
                            next: None,
                            prev: None,
                        },
                    )?;
                    let order_location = OrderLocation {
                        security_id : order.security_id,
                        is_buy_side : order.is_buy_side,
                        order_index : alloted_index
                    };
                    self._global_registry.insert(order.engine_order_id, order_location);
                    span.record("order_type", "limit");
                    span.record("is_buy_side", false);
                    span.record("levels_touched", levels_touched);
                    span.record("order_consumed", orders_consumed);
                }
            }
        } else {
            match order.order_type {
                OrderType::Market(None) => {
                    // need to immediatly execute the order on the best of other half
                    let mut fill_quantity = order.initial_quantity;
                    let mut levels_touched = 0;
                    let mut orders_consumed = 0;
                    while fill_quantity > 0 {
                        let remove_node: bool;
                        {
                            let Some(mut price_node) = orderbook.ask.price_map.last_entry() else {
                                break;
                            };
                            let price_level = price_node.get_mut();
                            while price_level.total_quantity > 0 && fill_quantity > 0 {
                                let head_idx = price_level.head;
                                let first_order_node =
                                    orderbook.ask.order_pool[head_idx].as_mut().unwrap();
                                if fill_quantity >= first_order_node.current_quantity {
                                    fill_quantity -= first_order_node.current_quantity;
                                    price_level.total_quantity -= first_order_node.current_quantity;
                                    let next = first_order_node.next;
                                    orderbook.ask.order_pool[head_idx] = None;
                                    orderbook.ask.free_list.push(head_idx);
                                    orders_consumed += 1;
                                    if let Some(next_order_idx) = next {
                                        price_level.head = next_order_idx;
                                    } else {
                                        span.record("reason", "exhausted");
                                        break;
                                    }
                                } else {
                                    first_order_node.current_quantity -= fill_quantity;
                                    price_level.total_quantity -= fill_quantity;
                                    fill_quantity = 0;
                                    span.record("filled", true);
                                }
                            }
                            remove_node = price_level.total_quantity == 0;
                        }
                        if remove_node {
                            orderbook.bid.price_map.pop_last();
                            levels_touched += 1;
                        }
                    }
                    span.record("order_type", "market");
                    span.record("is_buy_side", true);
                    span.record("levels_touched", levels_touched);
                    span.record("order_consumed", orders_consumed);
                }
                OrderType::Market(market_limit) => {
                    let mut fill_quantity = order.initial_quantity;
                    let mut levels_touched = 0;
                    let mut orders_consumed = 0;
                    while fill_quantity > 0 {
                        let remove_node: bool;
                        {
                            let Some(mut price_node) = orderbook.ask.price_map.last_entry() else {
                                break;
                            };
                            if market_limit.unwrap() <= *price_node.key() {
                                break;
                            }
                            let price_level = price_node.get_mut();
                            while price_level.total_quantity > 0 && fill_quantity > 0 {
                                let head_idx = price_level.head;
                                let first_order_node =
                                    orderbook.ask.order_pool[head_idx].as_mut().unwrap();
                                if fill_quantity >= first_order_node.current_quantity {
                                    fill_quantity -= first_order_node.current_quantity;
                                    price_level.total_quantity -= first_order_node.current_quantity;
                                    let next = first_order_node.next;
                                    orderbook.ask.order_pool[head_idx] = None;
                                    orderbook.ask.free_list.push(head_idx);
                                    orders_consumed += 1;
                                    if let Some(next_order_idx) = next {
                                        price_level.head = next_order_idx;
                                    } else {
                                        span.record("reason", "exhausted");
                                        break;
                                    }
                                } else {
                                    first_order_node.current_quantity -= fill_quantity;
                                    price_level.total_quantity -= fill_quantity;
                                    fill_quantity = 0;
                                    span.record("filled", true);
                                }
                            }
                            remove_node = price_level.total_quantity == 0;
                        }
                        if remove_node {
                            orderbook.bid.price_map.pop_last();
                            levels_touched += 1;
                        }
                    }
                    span.record("order_type", "market");
                    span.record("is_buy_side", true);
                    span.record("levels_touched", levels_touched);
                    span.record("order_consumed", orders_consumed);
                }
                OrderType::Limit => {
                    let mut fill_quantity = order.initial_quantity;
                    let mut levels_touched = 0;
                    let mut orders_consumed = 0;
                    while fill_quantity > 0 {
                        let remove_node: bool;
                        {
                            let Some(mut price_node) = orderbook.ask.price_map.last_entry() else {
                                break;
                            };
                            if order.price <= *price_node.key() {
                                break;
                            }
                            let price_level = price_node.get_mut();
                            while price_level.total_quantity > 0 && fill_quantity > 0 {
                                let head_idx = price_level.head;
                                let first_order_node =
                                    orderbook.ask.order_pool[head_idx].as_mut().unwrap();
                                if fill_quantity >= first_order_node.current_quantity {
                                    fill_quantity -= first_order_node.current_quantity;
                                    price_level.total_quantity -= first_order_node.current_quantity;
                                    let next = first_order_node.next;
                                    orderbook.ask.order_pool[head_idx] = None;
                                    orderbook.ask.free_list.push(head_idx);
                                    orders_consumed += 1;
                                    if let Some(next_order_idx) = next {
                                        price_level.head = next_order_idx;
                                    } else {
                                        span.record("reason", "partially_filled");
                                        break;
                                    }
                                } else {
                                    first_order_node.current_quantity -= fill_quantity;
                                    price_level.total_quantity -= fill_quantity;
                                    fill_quantity = 0;
                                }
                            }
                            remove_node = price_level.total_quantity == 0;
                        }
                        if remove_node {
                            orderbook.bid.price_map.pop_last();
                            levels_touched += 1;
                        }
                    }
                    let alloted_index = orderbook.create_buy_order(
                        order.engine_order_id,
                        OrderNode {
                            initial_quantity: order.initial_quantity,
                            current_quantity: fill_quantity,
                            market_limit: order.price,
                            next: None,
                            prev: None,
                        },
                    )?;
                    let order_location = OrderLocation {
                        security_id : order.security_id,
                        is_buy_side : order.is_buy_side,
                        order_index : alloted_index
                    };
                    self._global_registry.insert(order.engine_order_id, order_location);
                    span.record("order_type", "limit");
                    span.record("is_buy_side", true);
                    span.record("levels_touched", levels_touched);
                    span.record("order_consumed", orders_consumed);
                }
            }
        }
        Ok(())
    }
}
