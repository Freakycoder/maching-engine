use tracing::Span;
use crate::order_book::{orderbook::OrderBook, types::{NewOrder, OrderNode, OrderType}};


#[derive(Debug)]
pub struct MatchingEngine{
    _orderbook : OrderBook,
}

impl MatchingEngine {
    pub fn match_order(&mut self, order : NewOrder, span : &Span) -> Result<(), anyhow::Error>{
        if !order.is_buy_side { // for ASK order
            match order.order_type {
                OrderType::Market(None) => {
                    // need to immediatly execute the order on the best of other half
                    let mut fill_quantity = order.quantity;
                    let mut levels_touched = 0;
                    let mut orders_consumed = 0;
                    while fill_quantity > 0 {
                        let remove_node: bool;
                        {
                            let Some(mut price_node) = self._orderbook.bid.price_map.first_entry()
                            else {
                                break;
                            };
                            let price_level = price_node.get_mut();
                            while price_level.total_quantity > 0 && fill_quantity > 0 {
                                let head_idx = price_level.head;
                                let first_order_node = self._orderbook.bid.order_pool[head_idx].as_mut().unwrap();
                                if fill_quantity >= first_order_node.current_quantity{
                                    fill_quantity -= first_order_node.current_quantity;
                                    price_level.total_quantity -= first_order_node.current_quantity;
                                    let next = first_order_node.next;
                                    self._orderbook.bid.order_pool[head_idx] = None;
                                    self._orderbook.bid.free_list.push(head_idx);
                                    orders_consumed += 1;
                                    if let Some(next_order_idx) = next{
                                        price_level.head = next_order_idx;
                                    }
                                    else {
                                        span.record("reason", "exhausted");
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
                        if remove_node{
                            self._orderbook.bid.price_map.pop_first();
                            levels_touched += 1;
                        }
                    };
                    if fill_quantity == 0 {
                        span.record("filled", true);
                    }
                    else {
                        span.record("filled", false);
                    }
                    span.record("order_type", "market");
                    span.record("is_buy_side", false);
                    span.record("levels_touched", levels_touched);
                    span.record("order_consumed", orders_consumed);
                }
                OrderType::Market(market_limit) => {
                    let mut fill_quantity = order.quantity;
                    while fill_quantity > 0 {
                        let remove_node: bool;
                        {
                            let Some(mut price_node) = self._orderbook.bid.price_map.first_entry()
                            else {
                                break;
                            };
                            if market_limit.unwrap() >= *price_node.key() {
                                break;
                            }
                            let price_level = price_node.get_mut();
                            while price_level.total_quantity > 0 && fill_quantity > 0 {
                                let head_idx = price_level.head;
                                let first_order_node = self._orderbook.bid.order_pool[head_idx].as_mut().unwrap();
                                if fill_quantity >= first_order_node.current_quantity{
                                    fill_quantity -= first_order_node.current_quantity;
                                    price_level.total_quantity -= fill_quantity;
                                    let next = first_order_node.next;
                                    self._orderbook.bid.order_pool[head_idx] = None;
                                    self._orderbook.bid.free_list.push(head_idx);
                                    if let Some(next_order_idx) = next{
                                        price_level.head = next_order_idx;
                                    }
                                    else {
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
                        if remove_node{
                            self._orderbook.bid.price_map.pop_first();
                        }
                    };
                }
                OrderType::Limit => {
                    let mut fill_quantity = order.quantity;
                    while fill_quantity > 0 {
                        let remove_node: bool;
                        {
                            let Some(mut price_node) = self._orderbook.bid.price_map.first_entry()
                            else {
                                break;
                            };
                            if order.price >= *price_node.key() {
                                break;
                            }
                            let price_level = price_node.get_mut();
                            while price_level.total_quantity > 0 && fill_quantity > 0 {
                                let head_idx = price_level.head;
                                let first_order_node = self._orderbook.bid.order_pool[head_idx].as_mut().unwrap();
                                if fill_quantity >= first_order_node.current_quantity{
                                    fill_quantity -= first_order_node.current_quantity;
                                    price_level.total_quantity -= fill_quantity;
                                    let next = first_order_node.next;
                                    self._orderbook.bid.order_pool[head_idx] = None;
                                    self._orderbook.bid.free_list.push(head_idx);
                                    if let Some(next_order_idx) = next{
                                        price_level.head = next_order_idx;
                                    }
                                    else {
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
                        if remove_node{
                            self._orderbook.bid.price_map.pop_first();
                        }
                    };
                    if let Err(_) = self._orderbook.create_sell_order(OrderNode { order_id: order.order_id,
                        initial_quantity : order.quantity,
                        current_quantity : fill_quantity,
                        market_limit : order.price,
                        next : None,
                        prev : None}){
                            // log the error for creating a partially filled BUY order.
                        };
                }
            }
        }
        else {
            match order.order_type {
                OrderType::Market(None) => {
                    // need to immediatly execute the order on the best of other half
                    let mut fill_quantity = order.quantity;
                    while fill_quantity > 0 {
                        let remove_node: bool;
                        {
                            let Some(mut price_node) = self._orderbook.ask.price_map.last_entry()
                            else {
                                break;
                            };
                            let price_level = price_node.get_mut();
                            while price_level.total_quantity > 0 && fill_quantity > 0 {
                                let head_idx = price_level.head;
                                let first_order_node = self._orderbook.ask.order_pool[head_idx].as_mut().unwrap();
                                if fill_quantity >= first_order_node.current_quantity{
                                    fill_quantity -= first_order_node.current_quantity;
                                    price_level.total_quantity -= fill_quantity;
                                    let next = first_order_node.next;
                                    self._orderbook.ask.order_pool[head_idx] = None;
                                    self._orderbook.ask.free_list.push(head_idx);
                                    if let Some(next_order_idx) = next{
                                        price_level.head = next_order_idx;
                                    }
                                    else {
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
                        if remove_node{
                            self._orderbook.ask.price_map.pop_last();
                        }
                    };
                }
                OrderType::Market(market_limit) => {
                    let mut fill_quantity = order.quantity;
                    while fill_quantity > 0 {
                        let remove_node: bool;
                        {
                            let Some(mut price_node) = self._orderbook.ask.price_map.last_entry()
                            else {
                                break;
                            };
                            if market_limit.unwrap() <= *price_node.key() {
                                break;
                            }
                            let price_level = price_node.get_mut();
                            while price_level.total_quantity > 0 && fill_quantity > 0 {
                                let head_idx = price_level.head;
                                let first_order_node = self._orderbook.ask.order_pool[head_idx].as_mut().unwrap();
                                if fill_quantity >= first_order_node.current_quantity{
                                    fill_quantity -= first_order_node.current_quantity;
                                    price_level.total_quantity -= fill_quantity;
                                    let next = first_order_node.next;
                                    self._orderbook.ask.order_pool[head_idx] = None;
                                    self._orderbook.ask.free_list.push(head_idx);
                                    if let Some(next_order_idx) = next{
                                        price_level.head = next_order_idx;
                                    }
                                    else {
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
                        if remove_node{
                            self._orderbook.ask.price_map.pop_last();
                        }
                    };
                }
                OrderType::Limit => {
                    let mut fill_quantity = order.quantity;
                    while fill_quantity > 0 {
                        let remove_node: bool;
                        {
                            let Some(mut price_node) = self._orderbook.ask.price_map.last_entry()
                            else {
                                break;
                            };
                            if order.price <= *price_node.key() {
                                break;
                            }
                            let price_level = price_node.get_mut();
                            while price_level.total_quantity > 0 && fill_quantity > 0 {
                                let head_idx = price_level.head;
                                let first_order_node = self._orderbook.ask.order_pool[head_idx].as_mut().unwrap();
                                if fill_quantity >= first_order_node.current_quantity{
                                    fill_quantity -= first_order_node.current_quantity;
                                    price_level.total_quantity -= fill_quantity;
                                    let next = first_order_node.next;
                                    self._orderbook.ask.order_pool[head_idx] = None;
                                    self._orderbook.ask.free_list.push(head_idx);
                                    if let Some(next_order_idx) = next{
                                        price_level.head = next_order_idx;
                                    }
                                    else {
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
                        if remove_node{
                            self._orderbook.ask.price_map.pop_last();
                        }
                    };
                    if let Err(_) = self._orderbook.create_buy_order(OrderNode { order_id: order.order_id,
                        initial_quantity : order.quantity,
                        current_quantity : fill_quantity,
                        market_limit : order.price,
                        next : None,
                        prev : None}){
                            // log the error for creating a partially filled BUY order.
                        };
                    }

            }
        }
        Ok(())
    }

   


}

