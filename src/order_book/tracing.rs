use tracing::{Span, info_span};
use uuid::Uuid;

pub struct Tracing {}

impl Tracing {
    pub fn match_order_span(
        order_id: Uuid,
        filled: bool,
        reason: &'static str,
        order_type: &'static str,
        is_buy_side: bool,
        levels_touched: u32,
        orders_consumed: u32,
    ) -> Span {
        info_span!("match_order", order_id = %order_id,
                    filled = %filled,
                    reason = %reason,
                    order_type = %order_type ,
                    is_buy_side = %is_buy_side,
                    levels_touched = %levels_touched,
                    orders_consumed = %orders_consumed
        )
    }
    pub fn modify_span(
        order_id: Uuid,
        filled: bool,
        reason: &'static str,
        modify_reason: &'static str,
        intermediate_error : &'static str,
        order_type: &'static str,
        is_buy_side: bool,
        levels_touched: u32,
        orders_consumed: u32,
    ) -> Span {
        info_span!("modify", order_id = %order_id,
                    filled = %filled,
                    reason = %reason,
                    modify_reason = %modify_reason,
                    intermediate_error = %intermediate_error,
                    order_type = %order_type ,
                    is_buy_side = %is_buy_side,
                    levels_touched = %levels_touched,
                    orders_consumed = %orders_consumed
        )
    }
}
