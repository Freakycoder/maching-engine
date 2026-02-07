use std::collections::HashMap;
use uuid::Uuid;

#[derive(Debug)]
pub struct OrderNode{
    pub initial_quantity : u32,
    pub current_quantity : u32,
    pub market_limit : u32, // essentially the limit price at which the order gets executed
    pub next : Option<usize>,
    pub prev : Option<usize>
}


#[derive(Debug)]
pub struct NewOrder{
    pub engine_order_id : Uuid, // changes, could be multiple order for different assets/security
    pub price : u32, // need to check about the price ticks
    pub initial_quantity : u32,
    pub current_quantity : u32,
    pub is_buy_side : bool,
    pub security_id : Uuid,
    pub order_type : OrderType
}

#[derive(Debug)]
pub enum OrderType{
    Market(Option<u32>), // No cieling/floor price. leftover quantity is canceled
    Limit
}

#[derive(Debug)]
pub struct CancelOrder{
    pub is_buy_side : bool,
    pub order_index : usize
}

#[derive(Debug)]
pub struct ModifyOrder{ //THINK ABOUT CANCEL AND NOT CANCEL SCENARIO
    pub order_index : usize,
    pub is_buy_side : bool,
    pub new_price : Option<u32>,
    pub new_quantity : Option<u32>,
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

#[derive(Debug)]
pub struct GlobalOrderRegistry{
    pub map : HashMap<Uuid, OrderLocation>
}

impl GlobalOrderRegistry {
    pub fn new() -> Self{
        Self { map: HashMap::new() }
    }

    pub fn get_details(&self, global_order_id :&Uuid) -> Option<&OrderLocation>{
        let Some(order_details) = self.map.get(global_order_id)
        else {
            return None; // if its a early then we need to write return along with ';'
        };
        Some(order_details) // this is the final expression so no need return ;
    }
    pub fn delete(&mut self, global_order_id :&Uuid) -> Option<OrderLocation>{
        self.map.remove(global_order_id)
    }

    pub fn insert(&mut self, global_order_id :Uuid, orderlocation : OrderLocation) -> Option<OrderLocation>{
        self.map.insert(global_order_id, orderlocation)
    }
}

#[derive(Debug)]
pub struct OrderLocation{
    pub security_id : Uuid,
    pub is_buy_side : bool,
    pub order_index : usize
}

#[derive(Debug)]
pub enum ModifyOutcome{
    Inplace,
    Repriced {
        new_price : u32,
        old_initial_qty : u32,
        old_current_qty : u32
    },
    Requantized {
        old_price : u32,
        new_initial_qty : u32,
        old_current_qty : u32
    },
    Both {
        new_price : u32,
        new_initial_qty : u32,
        old_current_qty : u32
    }
}

#[derive(Debug)]
pub enum CancelOutcome {
    Success,
    Failed
}



