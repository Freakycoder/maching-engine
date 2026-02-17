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
        levels_consumed: Empty,
        orders_touched: Empty,
        actual_time : Empty
    ) -> Span {
        info_span!("match_order", order_id = %order_id,
                    filled = filled,
                    reason = reason,
                    order_type = %order_type ,
                    is_buy_side = %is_buy_side,
                    levels_consumed = levels_consumed,
                    orders_touched = orders_touched,
                    actual_time = actual_time
        )
    }
    pub fn modify_span(
        order_id: String,
        filled: bool,
        reason: Empty,
        modify_reason: Empty,
        intermediate_error : Empty,
        order_type: &'static str,
        is_buy_side: bool,
        levels_consumed: u32,
        orders_touched: u32,
    ) -> Span {
        info_span!("modify", order_id = %order_id,
                    filled = %filled,
                    reason = reason,
                    modify_reason = modify_reason,
                    intermediate_error = intermediate_error,
                    order_type = %order_type ,
                    is_buy_side = %is_buy_side,
                    levels_consumed = %levels_consumed,
                    orders_touched = %orders_touched
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
    pub fn depth_span(
        security_id: Empty,
        status: Empty,
        reason: Empty,
    ) -> Span{
        info_span!("depth", security_id = security_id,
                    status = status,
                    reason = reason,
        )
    }
}
