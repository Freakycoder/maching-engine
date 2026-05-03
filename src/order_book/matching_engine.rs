use crate::order_book::{
    orderbook::OrderBook, types::{
        BookDepth, CancelOutcome, EngineCancelOrder, EngineModifyOrder, EngineNewOrder, MatchOutcome, ModifyOutcome, OrderNode, OrderType
    }
};
use anyhow::{Context, anyhow};
use std::collections::HashMap;
use tracing::{Span};

#[derive(Debug)]
pub struct MatchingEngine {
    _book: HashMap<u32, OrderBook>
}

impl MatchingEngine {

    pub fn new() -> Self{
        Self { _book: HashMap::new()}
    }

    fn get_orderbook(
        &mut self,
        security_id : u32
    ) -> Option<&mut OrderBook> {
        
        let Some(book) = self._book.get_mut(&security_id) else {
            return None;
        };
        Some(book)
    }

    pub fn modify(
        &mut self,
        order_id: u64,
        security_id : u32,
        new_price: Option<u32>,
        new_qty: Option<u32>,
        is_buy_side : bool,
        span: &Span,
    ) -> Result< &'static str, anyhow::Error> {
        let _gaurd = span.enter();
        let orderbook = self
            .get_orderbook(security_id)
            .context("Could not find the orderbook")?;
        if let Ok(potential_modfication) = orderbook.modify_order(
            order_id,
            EngineModifyOrder {
                order_id,
                security_id,
                new_price,
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
                        let _ = self.match_order(
                            EngineNewOrder {
                                engine_order_id: order_id,
                                price: Some(new_price),
                                initial_quantity: new_initial_qty,
                                current_quantity : old_current_qty,
                                is_buy_side,
                                security_id,
                                order_type: OrderType::Limit,
                            },
                        span);
                        return Ok("Both")
                    },
                    ModifyOutcome::Repriced { new_price, old_initial_qty, old_current_qty } => 
                        {
                        span.record("modify_outcome", "price");
                            let _ = self.match_order(
                            EngineNewOrder {
                                engine_order_id: order_id,
                                price: Some(new_price),
                                initial_quantity: old_initial_qty,
                                current_quantity : old_current_qty,
                                is_buy_side,
                                security_id,
                                order_type: OrderType::Limit,
                            },
                        span);
                        return Ok("Repriced")
                    },
                    ModifyOutcome::Requantized { old_price, new_initial_qty, old_current_qty } => {
                            let _ = self.match_order(
                            EngineNewOrder {
                                engine_order_id: order_id,
                                price: Some(old_price),
                                initial_quantity: new_initial_qty,
                                current_quantity : old_current_qty,
                                is_buy_side,
                                security_id,
                                order_type: OrderType::Limit,
                            }, span);
                            return Ok("Requantized")
                    },
                    ModifyOutcome::Inplace => {
                        span.record("modify_outcome", "qty reduction");
                        return Ok("Inplace")
                    }
                }
            }
            return Ok("No potential modification")
        } else {
            return Ok("No modification occured");
        }
    }

    pub fn cancel(&mut self, order_id: u64,security_id : u32, span: &Span, is_buy_side : bool) -> Result<CancelOutcome, anyhow::Error>{
        let orderbook = self
            .get_orderbook(security_id)
            .context("Could not find the orderbook")?;
        if let Err(_) = orderbook.cancel_order(order_id, EngineCancelOrder{is_buy_side,security_id, order_id}){
            span.record("reason", "orderbook cancellation failed");
            span.record("success_status", false);
            return Ok(CancelOutcome::Failed);
        }; 
        span.record("success_status", true);
        return Ok(CancelOutcome::Success);
    }

    pub fn depth(&self, security_id : u32, levels_count :Option<u32>, span: &Span ) -> Result<BookDepth, anyhow::Error>{
        let _gaurd = span.enter();
        span.record("security_id", security_id.to_string());
        let Some(order_book) = self._book.get(&security_id) else {
            span.record("status", "failed");
            span.record("reason", "orderbook doesn't exist");
            return Err(anyhow!(""))
        };
        match order_book.depth(levels_count){
            Ok(book_depth) => {
                span.record("status", "success");
                span.record("reason", "None");
                Ok(book_depth)
            },
            Err(e) => Err(anyhow!("{}", e))
        }
    }

    pub fn match_order(&mut self, order: EngineNewOrder, span: &Span) -> Result<MatchOutcome, anyhow::Error> {
        
        let _gaurd = span.enter();

        let orderbook = match self._book.get_mut(&order.security_id){
            Some(orderbook) => {
                orderbook
            }
            None => {
                self._book.entry(order.security_id).or_insert(OrderBook::new())
            }
        };

        if !order.is_buy_side {
            // for ASK order
            match order.order_type {
                OrderType::Market(None) => {
                    // need to immediatly execute the order on the best of other half
                    let mut fill_quantity = order.initial_quantity;
                    let mut levels_consumed = 0;
                    let mut orders_touched = 0;
                    while fill_quantity > 0 {
                        let remove_node: bool;
                        {
                            let Some(mut price_node) = orderbook.bid.price_map.last_entry() else {
                                break;
                            };
                            let price_level = price_node.get_mut();
                            while price_level.total_quantity > 0 && fill_quantity > 0 {
                                if let Some(head_idx) = price_level.head{

                                    match orderbook.bid.order_pool[head_idx].as_mut(){
                                        Some(first_order_node) => {

                                            if fill_quantity >= first_order_node.current_quantity {
                                                fill_quantity -= first_order_node.current_quantity;
                                                
                                                price_level.total_quantity = price_level.total_quantity.checked_sub(first_order_node.current_quantity).ok_or(anyhow!("error occured in sub of total qty - current qyt"))?;
                                                let next = first_order_node.next;
                                                orderbook.bid.order_pool[head_idx] = None;
                                                orderbook.bid.free_list.push(head_idx);
                                                orders_touched += 1;
                                                if let Some(next_order_idx) = next {
                                                    price_level.head = Some(next_order_idx);
                                                } else {
                                                    span.record("reason", "exhausted");
                                                    price_level.total_quantity = 0;
                                                    price_level.head = None;
                                                    price_level.tail = None;
                                                    price_level.order_count = 0;
                                                    break;
                                                }
                                            } else {
                                                first_order_node.current_quantity = first_order_node.current_quantity.checked_sub(fill_quantity).ok_or(anyhow!("error occured subtracting fnq - fq"))?;
                                                price_level.total_quantity = price_level.total_quantity.checked_sub(fill_quantity).ok_or(anyhow!("error occured subtracting fntq - fq"))?;
                                                fill_quantity = 0;
                                                orders_touched += 1;
                                                span.record("filled", true);
                                            }
                                        }
                                        None => {
                                            return Err(anyhow!("failed to get head_idx from order pool"));
                                        }
                                    };
                                }else {
                                    // price level has no head. i.e head = None
                                    break;
                                }
                            }
                            remove_node = price_level.total_quantity == 0;
                        }
                        if remove_node {
                            match orderbook.bid.price_map.pop_last(){
                                Some(_) => {
                                    levels_consumed += 1;
                                }
                                None => {
                                    break;
                                }
                            };
                        }
                    }
                    span.record("order_type", "market");
                    span.record("is_buy_side", false);
                    span.record("levels_consumed", levels_consumed);
                    span.record("orders_touched", orders_touched);
                    Ok(MatchOutcome{
                        order_index : None,
                        levels_consumed,
                        orders_touched
                    })
                }
                OrderType::Market(market_limit) => {
                    let mut fill_quantity = order.initial_quantity;
                    let mut levels_consumed = 0;
                    let mut orders_touched = 0;
                    while fill_quantity > 0 {
                        let remove_node: bool;
                        {
                            let Some(mut price_node) = orderbook.bid.price_map.last_entry() else {
                                break;
                            };

                            match market_limit {
                                Some(price) => {
                                    if price > *price_node.key(){
                                        break;
                                    }
                                }
                                None => {
                                    return Err(anyhow!("did not recieve price for market-limit(SELL)"))
                                }
                            }
                            let price_level = price_node.get_mut();
                            while price_level.total_quantity > 0 && fill_quantity > 0 {
                                if let Some(head_idx) = price_level.head{

                                    match orderbook.bid.order_pool[head_idx].as_mut(){
                                        Some(first_order_node) => {

                                            if fill_quantity >= first_order_node.current_quantity {
                                                fill_quantity -= first_order_node.current_quantity;
                                                price_level.total_quantity = price_level.total_quantity.checked_sub(first_order_node.current_quantity).ok_or(anyhow!("error occured in sub of total qty - current qyt"))?;
                                                let next = first_order_node.next;
                                                orderbook.bid.order_pool[head_idx] = None;
                                                orderbook.bid.free_list.push(head_idx);
                                                orders_touched += 1;
                                                if let Some(next_order_idx) = next {
                                                    price_level.head = Some(next_order_idx);
                                                } else {
                                                    span.record("reason", "exhausted");
                                                    price_level.total_quantity = 0;
                                                    price_level.head = None;
                                                    price_level.tail = None;
                                                    price_level.order_count = 0;
                                                    break;
                                                }
                                            } else {
                                                first_order_node.current_quantity = first_order_node.current_quantity.checked_sub(fill_quantity).ok_or(anyhow!("error occured subtracting fnq - fq"))?;
                                                price_level.total_quantity = price_level.total_quantity.checked_sub(fill_quantity).ok_or(anyhow!("error occured subtracting fntq - fq"))?;
                                                fill_quantity = 0;
                                                orders_touched += 1;
                                                span.record("filled", true);
                                            }
                                        }
                                        None => {
                                            return Err(anyhow!("failed to get head_idx from order pool"));
                                        }
                                    };
                                }else {
                                    break;
                                }
                            }
                            remove_node = price_level.total_quantity == 0;
                        }
                        if remove_node {
                            match orderbook.bid.price_map.pop_last(){
                                Some(_) => {
                                    levels_consumed += 1;
                                }
                                None => {
                                    break;
                                }
                            };
                        }
                    }
                    span.record("order_type", "market");
                    span.record("is_buy_side", false);
                    span.record("levels_consumed", levels_consumed);
                    span.record("orders_touched", orders_touched);
                    Ok(MatchOutcome{
                        order_index : None,
                        levels_consumed,
                        orders_touched
                    })
                }
                OrderType::Limit => {
                    let mut fill_quantity = order.initial_quantity;
                    let mut levels_consumed = 0;
                    let mut orders_touched = 0;
                    while fill_quantity > 0 {
                        let remove_node: bool;
                        {
                            let Some(mut price_node) = orderbook.bid.price_map.last_entry() else {
                                break;
                            };

                            match order.price {
                                Some(price) => {
                                    if price > *price_node.key(){
                                        break;
                                    }
                                }
                                None => {
                                    return Err(anyhow!("did not recieve price for limit order (SELL)"))
                                }
                            }
                            let price_level = price_node.get_mut();
                            while price_level.total_quantity > 0 && fill_quantity > 0 {
                                if let Some(head_idx) = price_level.head{
                                    match orderbook.bid.order_pool[head_idx].as_mut(){
                                        Some(first_order_node) => {
                                            if fill_quantity >= first_order_node.current_quantity {
                                        fill_quantity -= first_order_node.current_quantity;
                                        price_level.total_quantity = price_level.total_quantity.checked_sub(first_order_node.current_quantity).ok_or(anyhow!("error occured in sub of total qty - current qyt"))?;
                                        let next = first_order_node.next;
                                        orderbook.bid.order_pool[head_idx] = None;
                                        orderbook.bid.free_list.push(head_idx);
                                        orders_touched += 1;
                                        if let Some(next_order_idx) = next {
                                            price_level.head = Some(next_order_idx);
                                        } else {
                                            span.record("reason", "partially_filled");
                                            price_level.total_quantity = 0;
                                            price_level.head = None;
                                            price_level.tail = None;
                                            price_level.order_count = 0;
                                            break;
                                        }
                                    } else {
                                        first_order_node.current_quantity = first_order_node.current_quantity.checked_sub(fill_quantity).ok_or(anyhow!("error occured subtracting fnq - fq"))?;
                                        price_level.total_quantity = price_level.total_quantity.checked_sub(fill_quantity).ok_or(anyhow!("error occured subtracting fntq - fq"))?;
                                        fill_quantity = 0;
                                        orders_touched += 1;
                                        span.record("filled", true);
                                    }
                                        }
                                        None => {
                                            return Err(anyhow!("failed to get head_idx from order pool"));
                                        }
                                    };
                                }else {
                                    break;
                                }
                            }
                            remove_node = price_level.total_quantity == 0;
                        }
                        if remove_node {
                            match orderbook.bid.price_map.pop_last(){
                                Some(_) => {
                                    levels_consumed += 1;
                                }
                                None => {
                                    break;
                                }
                            };
                        }
                    }
                    if fill_quantity > 0 {
                        let alloted_index = orderbook.create_sell_order(
                            order.engine_order_id,
                            OrderNode {
                                order_id : order.engine_order_id,
                                initial_quantity: order.initial_quantity,
                                current_quantity: fill_quantity,
                                market_limit: order.price.unwrap(),
                                next: None,
                                prev: None,
                            },
                        )?;
                        span.record("order_type", "limit");
                        span.record("is_buy_side", false);
                        span.record("levels_consumed", levels_consumed);
                        span.record("orders_touched", orders_touched);
                        return Ok(MatchOutcome{
                        order_index : Some(alloted_index as u32),
                        levels_consumed,
                        orders_touched
                    })
                    }
                    Ok(MatchOutcome{
                        order_index : None,
                        levels_consumed,
                        orders_touched
                    })
                }
            }
        } else {
            match order.order_type {
                OrderType::Market(None) => {
                    // need to immediatly execute the order on the best of other half
                    let mut fill_quantity = order.initial_quantity;
                    let mut levels_consumed = 0;
                    let mut orders_touched = 0;
                    while fill_quantity > 0 {
                        let remove_node: bool;
                        {
                            let Some(mut price_node) = orderbook.ask.price_map.first_entry() else {
                                break;
                            };
                            let price_level = price_node.get_mut();
                            while price_level.total_quantity > 0 && fill_quantity > 0 {
                                if let Some(head_idx) = price_level.head{
                                    match orderbook.ask.order_pool[head_idx].as_mut(){
                                        Some(first_order_node) => {

                                            if fill_quantity >= first_order_node.current_quantity {
                                                fill_quantity -= first_order_node.current_quantity;
                                                price_level.total_quantity = price_level.total_quantity.checked_sub(first_order_node.current_quantity).ok_or(anyhow!("error occured in sub of total qty - current qyt"))?;
                                                let next = first_order_node.next;
                                                orderbook.ask.order_pool[head_idx] = None;
                                                orderbook.ask.free_list.push(head_idx);
                                                orders_touched += 1;
                                                if let Some(next_order_idx) = next {
                                                    price_level.head = Some(next_order_idx);
                                                } else {
                                                    span.record("reason", "exhausted");
                                                    price_level.total_quantity = 0;
                                                    price_level.head = None;
                                                    price_level.tail = None;
                                                    price_level.order_count = 0;
                                                    break;
                                                }
                                            } else {
                                                first_order_node.current_quantity = first_order_node.current_quantity.checked_sub(fill_quantity).ok_or(anyhow!("error occured subtracting fnq - fq"))?;
                                                price_level.total_quantity = price_level.total_quantity.checked_sub(fill_quantity).ok_or(anyhow!("error occured subtracting fntq - fq"))?;
                                                fill_quantity = 0;
                                                orders_touched += 1;
                                                span.record("filled", true);
                                            }
                                        }
                                        None => {
                                            return Err(anyhow!("failed to get head_idx from order pool"));
                                        }
                                    };
                                }
                                else {
                                    break;
                                }
                            }
                            remove_node = price_level.total_quantity == 0;
                        }
                        if remove_node {
                            match orderbook.ask.price_map.pop_first(){
                                Some(_) => {
                                    levels_consumed += 1;
                                }
                                None => {
                                    break;
                                }
                            };
                        }
                    }
                    span.record("order_type", "market");
                    span.record("is_buy_side", true);
                    span.record("levels_consumed", levels_consumed);
                    span.record("orders_touched", orders_touched);
                    Ok(MatchOutcome{
                        order_index : None,
                        levels_consumed,
                        orders_touched
                    })
                }
                OrderType::Market(market_limit) => {
                    let mut fill_quantity = order.initial_quantity;
                    let mut levels_consumed = 0;
                    let mut orders_touched = 0;
                    while fill_quantity > 0 {
                        let remove_node: bool;
                        {
                            let Some(mut price_node) = orderbook.ask.price_map.first_entry() else {
                                break;
                            };

                            match market_limit {
                                Some(price) => {
                                    if price < *price_node.key(){
                                        break;
                                    }
                                }
                                None => {
                                    return Err(anyhow!("did not recieve price for market-limit(BUY)"))
                                }
                            }
                            let price_level = price_node.get_mut();
                            while price_level.total_quantity > 0 && fill_quantity > 0 {
                                let head_pointer = price_level.head;
                                if let Some(head_idx) = head_pointer{
                                    match orderbook.ask.order_pool[head_idx].as_mut(){
                                        Some(first_order_node) => {

                                            if fill_quantity >= first_order_node.current_quantity {
                                                fill_quantity -= first_order_node.current_quantity;
                                                price_level.total_quantity = price_level.total_quantity.checked_sub(first_order_node.current_quantity).ok_or(anyhow!("error occured in sub of total qty - current qyt"))?;
                                                let next = first_order_node.next;
                                                orderbook.ask.order_pool[head_idx] = None;
                                                orderbook.ask.free_list.push(head_idx);
                                                orders_touched += 1;
                                                if let Some(next_order_idx) = next {
                                                    price_level.head = Some(next_order_idx);
                                                } else {
                                                    span.record("reason", "exhausted");
                                                    price_level.head = None;
                                                    price_level.total_quantity = 0;
                                                    price_level.head = None;
                                                    price_level.tail = None;
                                                    price_level.order_count = 0;
                                                    break;
                                                }
                                            } else {
                                                first_order_node.current_quantity = first_order_node.current_quantity.checked_sub(fill_quantity).ok_or(anyhow!("error occured subtracting fnq - fq"))?;
                                                price_level.total_quantity = price_level.total_quantity.checked_sub(fill_quantity).ok_or(anyhow!("error occured subtracting fntq - fq"))?;
                                                fill_quantity = 0;
                                                orders_touched += 1;
                                                span.record("filled", true);
                                            }
                                        }
                                        None => {
                                            return Err(anyhow!("failed to get head_idx from order pool"));
                                        }
                                    };
                                }
                                else {
                                    break;
                                }
                            }
                            remove_node = price_level.total_quantity == 0;
                        }
                        if remove_node {
                            match orderbook.ask.price_map.pop_first(){
                                Some(_) => {
                                    levels_consumed += 1;
                                }
                                None => {
                                    break;
                                }
                            };
                        }
                    }
                    span.record("order_type", "market");
                    span.record("is_buy_side", true);
                    span.record("levels_consumed", levels_consumed);
                    span.record("orders_touched", orders_touched);
                    Ok(MatchOutcome{
                        order_index : None,
                        levels_consumed,
                        orders_touched
                    })
                }
                OrderType::Limit => {
                    let mut fill_quantity = order.initial_quantity;
                    let mut levels_consumed = 0;
                    let mut orders_touched = 0;
                    while fill_quantity > 0 {
                        let remove_node: bool;
                        {
                            let Some(mut price_node) = orderbook.ask.price_map.first_entry() else {
                                break;
                            };

                            match order.price {
                                Some(price) => {
                                    if price < *price_node.key(){
                                        break;
                                    }
                                }
                                None => {
                                    return Err(anyhow!("did not recieve price for limit(BUY)"))
                                }
                            }
                            let price_level = price_node.get_mut();
                            while price_level.total_quantity > 0 && fill_quantity > 0 {
                                if let Some(head_idx) = price_level.head{

                                    match orderbook.ask.order_pool[head_idx].as_mut(){
                                        Some(first_order_node) => {

                                            if fill_quantity >= first_order_node.current_quantity {
                                                fill_quantity -= first_order_node.current_quantity;
                                                price_level.total_quantity = price_level.total_quantity.checked_sub(first_order_node.current_quantity).ok_or(anyhow!("error occured in sub of total qty - current qyt"))?;
                                                let next = first_order_node.next;
                                                orderbook.ask.order_pool[head_idx] = None;
                                                orderbook.ask.free_list.push(head_idx);
                                                orders_touched += 1;
                                                if let Some(next_order_idx) = next {
                                                    price_level.head = Some(next_order_idx);
                                                } else {
                                                    span.record("reason", "partially_filled");
                                                    price_level.total_quantity = 0;
                                                    price_level.head = None;
                                                    price_level.tail = None;
                                                    price_level.order_count = 0;
                                                    break;
                                                }
                                            } else {
                                                first_order_node.current_quantity = first_order_node.current_quantity.checked_sub(fill_quantity).ok_or(anyhow!("error occured subtracting fnq - fq"))?;
                                                price_level.total_quantity = price_level.total_quantity.checked_sub(fill_quantity).ok_or(anyhow!("error occured subtracting fntq - fq"))?;
                                                fill_quantity = 0;
                                                orders_touched += 1;
                                                span.record("filled", true);
                                            }
                                        }
                                        None => {
                                            return Err(anyhow!("failed to get head_idx from order pool"));
                                        }
                                    };
                                }else {
                                    break;
                                }
                            }
                            remove_node = price_level.total_quantity == 0;
                        }
                        if remove_node {
                            match orderbook.ask.price_map.pop_first(){
                                Some(_) => {
                                    levels_consumed += 1;
                                }
                                None => {
                                    break;
                                }
                            };
                        }
                    }
                    if fill_quantity > 0{
                        let alloted_index = orderbook.create_buy_order(
                            order.engine_order_id,
                            OrderNode {
                                order_id : order.engine_order_id,
                                initial_quantity: order.initial_quantity,
                                current_quantity: fill_quantity,
                                market_limit: order.price.unwrap(),
                                next: None,
                                prev: None,
                            },
                        )?;
                        span.record("order_type", "limit");
                        span.record("is_buy_side", true);
                        span.record("levels_consumed", levels_consumed);
                        span.record("orders_touched", orders_touched);
                        return Ok(MatchOutcome{
                        order_index : Some(alloted_index as u32),
                        levels_consumed,
                        orders_touched
                    })
                    }
                    Ok(MatchOutcome{
                        order_index : None,
                        levels_consumed,
                        orders_touched
                    })
                }
            }
        }
    }
}
