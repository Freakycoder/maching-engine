use std::{collections::{BTreeMap}};
use tracing::instrument;

use crate::order_book::types::{CancelOrder, ModifyOrder, OrderNode, OrderRegistry,PriceLevel};

#[derive(Debug)]
pub struct OrderBook{
    pub asset_id : String,
    pub ask : HalfBook,
    pub bid : HalfBook
}
impl OrderBook {
    pub fn new (name : String,) -> Self{
        Self { asset_id : name , ask : HalfBook::new(), bid : HalfBook::new() }
    }

    #[instrument( // used for auto span creation & drop.
        name = "create_buy_order",
        skip(self),
        fields(
            order_id = %resting_order.order_id,
            price = resting_order.market_limit 
        ),
        err
    )]
    pub fn create_buy_order(&mut self, resting_order : OrderNode) -> Result<(), anyhow::Error>{
        
        let mut order = resting_order;
        let order_id = order.order_id;
        let order_quantity = order.current_quantity;
        let price = order.market_limit;
        
        if let Some(price_level) = self.bid.price_map.get_mut(&price){
            order.prev = Some(price_level.tail);
            if let Some(free_index) = self.bid.free_list.pop(){
                self.bid.order_pool.insert(free_index, Some(order));
                self.bid.order_registry.insert(order_id, free_index);
                price_level.tail = free_index;
                if let Some(prev_order) = self.bid.order_pool.get_mut(price_level.tail).unwrap(){
                    prev_order.next = Some(free_index);
                };
                return Ok(());
            }
            else {
            self.bid.order_pool.push(Some(order));
            let new_tail = self.bid.order_pool.len() - 1;
            self.bid.order_registry.insert(order_id, new_tail);
            price_level.tail = new_tail;
            if let Some(prev_order) = self.bid.order_pool.get_mut(price_level.tail).unwrap(){
                prev_order.next = Some(new_tail);
            };
            return Ok(());
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
            self.bid.order_registry.insert(order_id, free_index);
            new_price_level.head = free_index;
            new_price_level.tail = free_index;
            new_price_level.order_count += 1;
            new_price_level.total_quantity += order_quantity;
            self.bid.price_map.entry(price).or_insert(new_price_level);
            return Ok(())
        }
        self.bid.order_pool.push(Some(order));
        let new_index = self.bid.order_pool.len()-1;
        self.bid.order_registry.insert(order_id, new_index);
        new_price_level.head = new_index;
        new_price_level.tail = new_index;
        new_price_level.order_count += 1;
        new_price_level.total_quantity += order_quantity;
        self.bid.price_map.entry(price).or_insert(new_price_level);
        
        Ok(())
    }

    #[instrument( 
        name = "create_sell_order",
        skip(self),
        fields(
            order_id = %resting_order.order_id,
            price = resting_order.market_limit 
        ),
        err
    )]
    pub fn create_sell_order(&mut self, resting_order : OrderNode) -> Result<(), anyhow::Error>{
        let mut order = resting_order;
        let order_id = order.order_id;
        let order_quantity = order.current_quantity;
        let price = order.market_limit;
        
        if let Some(price_level) = self.ask.price_map.get_mut(&price){
            order.prev = Some(price_level.tail);
            if let Some(free_index) = self.ask.free_list.pop(){
                self.ask.order_pool.insert(free_index, Some(order));
                self.ask.order_registry.insert(order_id, free_index);
                price_level.tail = free_index;
                if let Some(prev_order) = self.ask.order_pool.get_mut(price_level.tail).unwrap(){
                    prev_order.next = Some(free_index);
                };
                return Ok(());
            }
            else {
            self.ask.order_pool.push(Some(order));
            let new_tail = self.ask.order_pool.len() - 1;
            self.ask.order_registry.insert(order_id, new_tail);
            price_level.tail = new_tail;
            if let Some(prev_order) = self.ask.order_pool.get_mut(price_level.tail).unwrap(){
                prev_order.next = Some(new_tail);
            };
            return Ok(());
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
            self.ask.order_registry.insert(order_id, free_index);
            new_price_level.head = free_index;
            new_price_level.tail = free_index;
            new_price_level.order_count += 1;
            new_price_level.total_quantity += order_quantity;
            self.ask.price_map.entry(price).or_insert(new_price_level);
            return Ok(())
        }
        self.ask.order_pool.push(Some(order));
        let new_index = self.ask.order_pool.len()-1;
        self.ask.order_registry.insert(order_id, new_index);
        new_price_level.head = new_index;
        new_price_level.tail = new_index;
        new_price_level.order_count += 1;
        new_price_level.total_quantity += order_quantity;
        self.ask.price_map.entry(price).or_insert(new_price_level);
        
        Ok(())
    }

    #[instrument( 
        name = "cancel_order",
        skip(self),
        fields(
            order_id = %order.order_id
        ),
        err
    )]
    pub fn cancel_order(&mut self, order : CancelOrder) -> Result<(), anyhow::Error>{
        if order.is_buy_side {
           if self.bid.order_registry.order_exist(order.order_id){
                if let Some(deleted_index) = self.bid.order_registry.delete_key(order.order_id){
                    let (prev, next) = {
                        let node = self.bid.order_pool[deleted_index].as_ref().unwrap();
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
                    self.bid.order_pool.insert(deleted_index, None);
                    self.bid.free_list.push(deleted_index);
                }
           }
        } else {
           if self.ask.order_registry.order_exist(order.order_id){
                if let Some(deleted_index) = self.ask.order_registry.delete_key(order.order_id){
                    let (prev, next) = {
                        let node = self.ask.order_pool[deleted_index].as_ref().unwrap();
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
                    self.ask.order_pool.insert(deleted_index, None);
                    self.ask.free_list.push(deleted_index);
                }
           }
        }
        Ok(())
    }

    #[instrument( 
        name = "modify_order",
        skip(self),
        fields(
            order_id = %order.order_id,
        ),
        err
    )]
    pub fn modify_order(&mut self, order : ModifyOrder) -> Result<(), anyhow::Error>{
        if order.is_buy_side{
            if self.bid.order_registry.order_exist(order.order_id){
                let idx = self.bid.order_registry.get_idx(order.order_id);
                let order_node = {
                    let node = self.bid.order_pool[*idx].as_mut().unwrap();
                    node
                };
                if order.new_price != order_node.market_limit || order.new_quantity > order_node.initial_quantity{
                    if let Err(_) = self.cancel_order(CancelOrder { order_id: order.order_id, is_buy_side: order.is_buy_side, security_id: order.security_id }){
                        // log the fail message - "failed to cancel the modify order"
                    };
                    if let Err(_) = self.create_buy_order(OrderNode {order_id: order.order_id,
                        initial_quantity : order.new_quantity,
                        current_quantity : order.new_quantity,
                        market_limit : order.new_price,
                        next : None,
                        prev : None })
                        {
                        // log the fail message for creating new BUY order
                        }
                    // succesfully cancelled and created new order
                    return Ok(())
                } else {
                    order_node.current_quantity = order.new_quantity;
                    return Ok(());
                }
            }
            // log cancel order doesnt exist
            return Ok(());
        } else {
            if self.ask.order_registry.order_exist(order.order_id){
                let idx = self.ask.order_registry.get_idx(order.order_id);
                let order_node = {
                    let node = self.ask.order_pool[*idx].as_mut().unwrap();
                    node
                };
                if order.new_price != order_node.market_limit || order.new_quantity > order_node.initial_quantity{
                    if let Err(_) = self.cancel_order(CancelOrder { order_id: order.order_id, is_buy_side: order.is_buy_side, security_id : order.security_id }){
                        // log the fail message - "failed to cancel the modify order"
                    };
                    if let Err(_) = self.create_sell_order(OrderNode {order_id: order.order_id,
                        initial_quantity : order.new_quantity,
                        current_quantity : order.new_quantity,
                        market_limit : order.new_price,
                        next : None,
                        prev : None })
                        {
                        // log the fail message for creating new SELL order
                        }
                    // succesfully cancelled and created new order
                    return Ok(())
                } else {
                    order_node.current_quantity = order.new_quantity;
                    return Ok(())
                }
            }
            // log cancel order doesnt exist
            return Ok(());
        }
    }
}

#[derive(Debug)]
pub struct HalfBook{
    pub price_map : BTreeMap<u32, PriceLevel>,
    pub order_pool : Vec<Option<OrderNode>>,
    pub free_list : Vec<usize>, // we're storing the free indices from the price level to keep the cache lines hot.
    order_registry : OrderRegistry
}

impl HalfBook {
    pub fn new() -> Self{
        Self { price_map: BTreeMap::new(), order_pool: Vec::new(), free_list: Vec::new(), order_registry : OrderRegistry::new() }
    }
}