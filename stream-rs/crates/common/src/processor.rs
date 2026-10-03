use crate::event::InputEvent;

pub trait Processor {
    type Event: InputEvent;
    type State: Default;
    type Output;

    fn on_event(&mut self, event: &Self::Event, state: &mut Self::State) -> Vec<Self::Output>;
    fn on_watermark(
        &mut self,
        vin: &str,
        watermark_ms: i64,
        state: &mut Self::State,
    ) -> Vec<Self::Output>;
    fn is_open(&self, state: &Self::State) -> bool;
}
