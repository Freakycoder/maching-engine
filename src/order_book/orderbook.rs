use std::collections::BTreeMap;
use tracing::instrument;
use uuid::Uuid;

use crate::order_book::types::{BookDepth, CancelOrder, ModifyOrder, ModifyOutcome, OrderNode, PriceLevel, PriceLevelDepth};

#[derive(Debug)]
pub struct OrderBook{
    pub security_id : u32,
    pub ask : HalfBook,
    pub bid : HalfBook
}
impl OrderBook {
    pub fn new (security_id : u32,) -> Self{
        Self { security_id , ask : HalfBook::new(), bid : HalfBook::new() }
    }

    #[instrument( // used for auto span creation & drop.
        name = "create_buy_order",
        skip(self),
        fields(
            order_id = %order_id,
            price = resting_order.market_limit
        ),
        err
    )]
    pub fn create_buy_order(&mut self, order_id : Uuid, resting_order : OrderNode) -> Result<usize, anyhow::Error>{
        
        let mut order = resting_order;
        let order_quantity = order.current_quantity;
        let price = order.market_limit;
        
        if let Some(price_level) = self.bid.price_map.get_mut(&price){
            order.prev = Some(price_level.tail);
            if let Some(free_index) = self.bid.free_list.pop(){
                self.bid.order_pool.insert(free_index, Some(order));
                price_level.tail = free_index;
                if let Some(prev_order) = self.bid.order_pool.get_mut(price_level.tail).unwrap(){
                    prev_order.next = Some(free_index);
                };
                return Ok(free_index);
            }
            else {
            self.bid.order_pool.push(Some(order));
            let new_tail = self.bid.order_pool.len() - 1;
            price_level.tail = new_tail;
            if let Some(prev_order) = self.bid.order_pool.get_mut(price_level.tail).unwrap(){
                prev_order.next = Some(new_tail);
            };
            return Ok(new_tail);
            }
        }

        let mut new_price_level = PriceLevel{
            head : 0,
            tail : 0,
            order_count : 0,
            total_quantity : 0
        };
        if let Some(free_index) = self.bid.free_list.pop(){
            self.bid.order_pool.insert(free_index, Some(order));
            new_price_level.head = free_index;
            new_price_level.tail = free_index;
            new_price_level.order_count += 1;
            new_price_level.total_quantity += order_quantity;
            self.bid.price_map.entry(price).or_insert(new_price_level);
            return Ok(free_index)
        }
        self.bid.order_pool.push(Some(order));
        let new_index = self.bid.order_pool.len()-1;
        new_price_level.head = new_index;
        new_price_level.tail = new_index;
        new_price_level.order_count += 1;
        new_price_level.total_quantity += order_quantity;
        self.bid.price_map.entry(price).or_insert(new_price_level);
        
        Ok(new_index)
    }

    #[instrument( 
        name = "create_sell_order",
        skip(self),
        fields(
            order_id = %order_id,
            price = resting_order.market_limit 
        ),
        err
    )]
    pub fn create_sell_order(&mut self, order_id : Uuid, resting_order : OrderNode) -> Result<usize, anyhow::Error>{
        let mut order = resting_order;
        let order_quantity = order.current_quantity;
        let price = order.market_limit;
        
        if let Some(price_level) = self.ask.price_map.get_mut(&price){
            order.prev = Some(price_level.tail);
            if let Some(free_index) = self.ask.free_list.pop(){
                self.ask.order_pool.insert(free_index, Some(order));
                price_level.tail = free_index;
                if let Some(prev_order) = self.ask.order_pool.get_mut(price_level.tail).unwrap(){
                    prev_order.next = Some(free_index);
                };
                return Ok(free_index);
            }
            else {
            self.ask.order_pool.push(Some(order));
            let new_tail = self.ask.order_pool.len() - 1;
            price_level.tail = new_tail;
            if let Some(prev_order) = self.ask.order_pool.get_mut(price_level.tail).unwrap(){
                prev_order.next = Some(new_tail);
            };
            return Ok(new_tail);
            }
        }

        let mut new_price_level = PriceLevel{
            head : 0,
            tail : 0,
            order_count : 0,
            total_quantity : 0
        };
        if let Some(free_index) = self.ask.free_list.pop(){
            self.ask.order_pool.insert(free_index, Some(order));
            new_price_level.head = free_index;
            new_price_level.tail = free_index;
            new_price_level.order_count += 1;
            new_price_level.total_quantity += order_quantity;
            self.ask.price_map.entry(price).or_insert(new_price_level);
            return Ok(free_index)
        }
        self.ask.order_pool.push(Some(order));
        let new_index = self.ask.order_pool.len()-1;
        new_price_level.head = new_index;
        new_price_level.tail = new_index;
        new_price_level.order_count += 1;
        new_price_level.total_quantity += order_quantity;
        self.ask.price_map.entry(price).or_insert(new_price_level);
        
        Ok(new_index)
    }

    #[instrument( 
        name = "cancel_order",
        skip(self),
        fields(
            order_id = %order_id
        ),
        err
    )]
    pub fn cancel_order(&mut self, order_id : Uuid, order : CancelOrder) -> Result<(), anyhow::Error>{
        if order.is_buy_side {
                    let (prev, next) = {
                        let node = self.bid.order_pool[order.order_index].as_ref().unwrap();
                        (node.prev, node.next)
                    };
                    if let Some(prev_index) = prev{
                        if let Some(possible_prev_node) = self.bid.order_pool.get_mut(prev_index){
                            if let Some(prev_node) = possible_prev_node{
                                prev_node.next = next
                            }
                        }
                    }
                    if let Some(next_index) = next{
                        if let Some(possible_next_node) = self.bid.order_pool.get_mut(next_index){
                            if let Some(next_node) = possible_next_node{
                                next_node.prev = prev
                            }
                        }
                    }
                    self.bid.order_pool.insert(order.order_index, None);
                    self.bid.free_list.push(order.order_index);
               
        } else {
                    let (prev, next) = {
                        let node = self.ask.order_pool[order.order_index].as_ref().unwrap();
                        (node.prev, node.next)
                    };
                    if let Some(prev_index) = prev{
                        if let Some(possible_prev_node) = self.ask.order_pool.get_mut(prev_index){
                            if let Some(prev_node) = possible_prev_node{
                                prev_node.next = next
                            }
                        }
                    }
                    if let Some(next_index) = next{
                        if let Some(possible_next_node) = self.ask.order_pool.get_mut(next_index){
                            if let Some(next_node) = possible_next_node{
                                next_node.prev = prev
                            }
                        }
                    }
                self.ask.order_pool.insert(order.order_index, None);
                self.ask.free_list.push(order.order_index);
        }
        Ok(())
    }

    #[instrument( 
        name = "modify_order",
        skip(self),
        fields(
            order_id = %order_id,
        ),
        err
    )]
    pub fn modify_order(&mut self, order_id : Uuid, order : ModifyOrder) -> Result<Option<ModifyOutcome>, anyhow::Error>{
        if order.is_buy_side{
                let (old_initial_qty, old_current_qty, old_price) = {
                    let node = self.bid.order_pool[order.order_index].as_ref().unwrap();
                    (node.initial_quantity, node.current_quantity, node.market_limit)
                };
                if let Some(new_price) = order.new_price && let Some(new_qty) = order.new_quantity{
                    if new_price != old_price{
                        if let Ok(_) = self.cancel_order(order_id ,CancelOrder { order_index : order.order_index, is_buy_side: order.is_buy_side,}){
                            return Ok(Some(ModifyOutcome::Both {new_price, new_initial_qty: new_qty, old_current_qty }));
                            }
                        }
                    return Ok(None);
                } else if let Some(new_qty) = order.new_quantity  {
                    if new_qty > old_initial_qty{
                        if let Ok(_) = self.cancel_order(order_id ,CancelOrder { order_index : order.order_index, is_buy_side: order.is_buy_side,}){
                            return Ok(Some(ModifyOutcome::Requantized {old_price, new_initial_qty: new_qty, old_current_qty }))
                        }
                        return Ok(None);
                    }
                    else {
                        let order_node = self.bid.order_pool[order.order_index].as_mut().unwrap();
                        order_node.initial_quantity = new_qty;
                        return Ok(Some(ModifyOutcome::Inplace));
                    }
                } else {
                    if let Ok(_) = self.cancel_order(order_id ,CancelOrder { order_index : order.order_index, is_buy_side: order.is_buy_side,}){
                        return Ok(Some(ModifyOutcome::Repriced {new_price : order.new_price.unwrap(), old_initial_qty, old_current_qty }));
                    }
                    return Ok(None);
                }
        } else {
                let (old_initial_qty, old_current_qty, old_price) = {
                    let node = self.ask.order_pool[order.order_index].as_ref().unwrap();
                    (node.initial_quantity, node.current_quantity, node.market_limit)
                };

                if let Some(new_price) = order.new_price && let Some(new_qty) = order.new_quantity{
                    if new_price != old_price{
                        if let Ok(_) = self.cancel_order(order_id ,CancelOrder { order_index : order.order_index, is_buy_side: order.is_buy_side,}){
                           return Ok(Some(ModifyOutcome::Requantized {old_price, new_initial_qty: new_qty, old_current_qty }))
                        }
                    }
                    return Ok(None);
                } else if let Some(new_qty) = order.new_quantity  {
                    if new_qty > old_initial_qty{
                        if let Ok(_) = self.cancel_order(order_id ,CancelOrder { order_index : order.order_index, is_buy_side: order.is_buy_side,}){
                            return Ok(Some(ModifyOutcome::Requantized { old_price, new_initial_qty: new_qty, old_current_qty }))
                        }
                        return Ok(None);
                    }
                    else {
                        let order_node = self.ask.order_pool[order.order_index].as_mut().unwrap();
                        order_node.initial_quantity = new_qty;
                        return Ok(Some(ModifyOutcome::Inplace));
                    }
                }else {
                    if let Ok(_) = self.cancel_order(order_id ,CancelOrder { order_index : order.order_index, is_buy_side: order.is_buy_side,}){
                        return Ok(Some(ModifyOutcome::Repriced { new_price : order.new_price.unwrap(), old_initial_qty, old_current_qty }));
                    }
                    return Ok(None);
                }
        }
    }
    
    #[instrument( 
        name = "book_depth",
        skip(self),
        err
    )]
    pub fn depth(&self, levels_count : Option<usize>) -> Result<BookDepth, anyhow::Error>{

        let ask_iter = self.ask.price_map.iter().rev();
        let bid_iter = self.bid.price_map.iter();

        let ask_depth : Vec<_> = match levels_count {
            Some(n) => ask_iter.take(n)
            .map(|(price, price_level)| PriceLevelDepth {
                price_level : *price,
                quantity : price_level.total_quantity
            })
            .collect(),
            None => ask_iter.map(|(price, price_level)| PriceLevelDepth {
                price_level : *price,
                quantity : price_level.total_quantity
            }).collect()
        };
        let bid_depth = match levels_count {
            Some(n) => bid_iter.take(n)
            .map(|(price, price_level)| PriceLevelDepth {
                price_level : *price,
                quantity : price_level.total_quantity
            })
            .collect(),
            None => bid_iter.map(|(price, price_level)| PriceLevelDepth {
                price_level : *price,
                quantity : price_level.total_quantity
            }).collect()
        };
        Ok(BookDepth { bid_depth, ask_depth })
    }
}

#[derive(Debug)]
pub struct HalfBook{
    pub price_map : BTreeMap<u32, PriceLevel>,
    pub order_pool : Vec<Option<OrderNode>>,
    pub free_list : Vec<usize>, // we're storing the free indices from the price level to keep the cache lines hot.
}

impl HalfBook {
    pub fn new() -> Self{
        Self { price_map: BTreeMap::new(), order_pool: Vec::new(), free_list: Vec::new()}
    }
}