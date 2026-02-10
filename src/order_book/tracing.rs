use tracing::{Span, info_span};
use uuid::Uuid;
use tracing::field::Empty;

pub struct Tracing {}

impl Tracing {
    pub fn match_order_span(
        order_id: String,
        filled: Empty,
        reason: Empty,
        order_type: &'static str,
        is_buy_side: bool,
        levels_touched: Empty,
        orders_consumed: Empty,
        actual_time : Empty
    ) -> Span {
        info_span!("match_order", order_id = %order_id,
                    filled = filled,
                    reason = reason,
                    order_type = %order_type ,
                    is_buy_side = %is_buy_side,
                    levels_touched = levels_touched,
                    orders_consumed = orders_consumed,
                    actual_time = actual_time
        )
    }
    pub fn modify_span(
        order_id: String,
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

    pub fn cancel_span(
        order_id: Uuid,
        success_status: bool,
        reason: &'static str,
    ) -> Span{
        info_span!("cancel", order_id = %order_id,
                    success_status = %success_status,
                    reason = %reason,
        )
    }
}
