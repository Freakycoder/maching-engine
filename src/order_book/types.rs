use std::collections::HashMap;

#[derive(Debug, Copy, Clone)]
pub struct OrderNode{
    pub order_id : u64,
    pub initial_quantity : u32,
    pub current_quantity : u32,
    pub market_limit : u32, // essentially the limit or (market limit) price at which the order gets executed
    pub next : Option<usize>,
    pub prev : Option<usize>
}


#[derive(Debug)]
pub struct EngineNewOrder{
    pub engine_order_id : u64,
    pub price : Option<u32>, // price recieved over here are already in whole number
    pub initial_quantity : u32,
    pub current_quantity : u32,
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
pub struct EngineCancelOrder{
    pub order_id : u64,
    pub security_id : u32,
    pub is_buy_side : bool
}

#[derive(Debug)]
pub struct EngineModifyOrder{ //THINK ABOUT CANCEL AND NOT CANCEL SCENARIO
    pub order_id : u64,
    pub security_id : u32,
    pub is_buy_side : bool,
    pub new_price : Option<u32>,
    pub new_quantity : Option<u32>,
}

#[derive(Debug)]
pub struct OrderRegistry{
    _asset_view : HashMap<u64, usize>
}

impl OrderRegistry {
    pub fn new() -> Self{
        Self { _asset_view: HashMap::new() }
    }
    pub fn insert(&mut self, order_id : u64, idx : usize) -> Option<usize>{
        self._asset_view.insert(order_id, idx)
    }
    pub fn order_exist(&self, order_id : u64) -> bool{
        self._asset_view.contains_key(&order_id)
    }
    pub fn get_idx(&self, order_id : u64) -> &usize{
        self._asset_view.get(&order_id).unwrap()
    }
    pub fn delete_key(&mut self, order_id : u64) -> Option<usize>{
        self._asset_view.remove(&order_id)
    }
}

#[derive(Debug)]
pub struct PriceLevel{
    pub head : Option<usize>,
    pub tail : Option<usize>,
    pub order_count : u32,
    pub total_quantity : u32
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

#[derive(Debug)]
pub struct BookDepth{
    pub bid_depth : Vec<PriceLevelDepth>,
    pub ask_depth : Vec<PriceLevelDepth>
}

#[derive(Debug)]
pub struct PriceLevelDepth{
    pub price_level : u32,
    pub quantity : u32
}


