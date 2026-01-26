use std::collections::HashMap;

use uuid::Uuid;

#[derive(Debug)]
pub struct OrderNode{
    pub order_id : Uuid,
    pub initial_quantity : u32,
    pub current_quantity : u32,
    pub market_limit : u32, // essentially the limit price at which the order gets executed
    pub next : Option<usize>,
    pub prev : Option<usize>
}


#[derive(Debug)]
pub struct NewOrder{
    pub order_id : Uuid, // changes, could be multiple order for different assets/security
    pub price : u32, // need to check about the price ticks
    pub quantity : u32,
    pub is_buy_side : bool,
    pub security_id : u32,
    pub order_type : OrderType
}

#[derive(Debug)]
pub enum OrderType{
    Market(Option<u32>), // No cieling/floor price. leftover quantity is canceled
    Limit
}

#[derive(Debug)]
pub struct CancelOrder{
    pub order_id : Uuid,
    pub is_buy_side : bool
}

#[derive(Debug)]
pub struct ModifyOrder{ //THINK ABOUT CANCEL AND NOT CANCEL SCENARIO
    pub order_id : Uuid,
    pub security_id : u32,
    pub is_buy_side : bool,
    pub new_price : u32,
    pub new_quantity : u32,
}

#[derive(Debug)]
pub struct OrderRegistry{
    _asset_view : HashMap<Uuid, usize>
}

impl OrderRegistry {
    pub fn new() -> Self{
        Self { _asset_view: HashMap::new() }
    }
    pub fn insert(&mut self, order_id : Uuid, idx : usize) -> Option<usize>{
        self._asset_view.insert(order_id, idx)
    }
    pub fn order_exist(&self, order_id : Uuid) -> bool{
        self._asset_view.contains_key(&order_id)
    }
    pub fn get_idx(&self, order_id : Uuid) -> &usize{
        self._asset_view.get(&order_id).unwrap()
    }
    pub fn delete_key(&mut self, order_id : Uuid) -> Option<usize>{
        self._asset_view.remove(&order_id)
    }
}

#[derive(Debug)]
pub struct PriceLevel{
    pub head : usize,
    pub tail : usize,
    pub order_count : u32,
    pub total_quantity : u32
}


