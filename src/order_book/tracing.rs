use tracing::{Span, debug_span, error_span, info_span,warn_span};
use uuid::Uuid;

pub struct Tracing{}

impl Tracing {
    pub fn buy_order_span(order_id : Uuid) -> Span{
        info_span!("buy_order", order_id = %order_id)
    }
    pub fn sell_order_span(order_id : Uuid) -> Span{
        info_span!("sell_order", order_id = %order_id)
    }
    pub fn modify_order_span(order_id : Uuid) -> Span{
        info_span!("modify_order", order_id = %order_id)
    }
    pub fn cancel_order_span(order_id : Uuid) -> Span{
        info_span!("cancel_order", order_id = %order_id)
    }
    pub fn match_order_span(order_id : Uuid, filled : bool, reason : String, is_buy_side : bool, levels_touched : u8, orders_consumed : u8) -> Span{
        info_span!("match_order", order_id = %order_id, filled = %filled, reason = %reason, is_buy_side = %is_buy_side, levels_touched = %levels_touched, orders_consumed = %orders_consumed)
    }
}