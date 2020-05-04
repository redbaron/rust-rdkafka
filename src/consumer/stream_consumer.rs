//! Stream-based consumer implementation.

use std::marker::PhantomPinned;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use rdkafka_sys as rdsys;
use rdkafka_sys::types::RDKafka;
use std::os::raw::c_void;

use futures::Stream;
use log::trace;

use std::sync::Mutex;

use crate::config::{ClientConfig, FromClientConfig, FromClientConfigAndContext};
use crate::consumer::base_consumer::BaseConsumer;
use crate::consumer::{Consumer, ConsumerContext, DefaultConsumerContext};
use crate::error::KafkaResult;
use crate::message::BorrowedMessage;
use crate::util::Timeout;

struct QueueNonEmptyCbArg {
    waker: Mutex<Option<futures::task::Waker>>,
    _pin: PhantomPinned,
}

impl QueueNonEmptyCbArg {
    fn new() -> Pin<Box<Self>> {
        Box::pin(QueueNonEmptyCbArg {
            waker: Mutex::new(None),
            _pin: PhantomPinned,
        })
    }
}

/// A Kafka Consumer providing a `futures::Stream` interface.
///
/// This consumer doesn't need to be polled since it has a separate polling thread. Due to the
/// asynchronous nature of the stream, some messages might be consumed by the consumer without being
/// processed on the other end of the stream. If auto commit is used, it might cause message loss
/// after consumer restart. Manual offset storing should be used, see the `store_offset` function on
/// `Consumer`.
#[must_use = "Consumer polling thread will stop immediately if unused"]
pub struct StreamConsumer<C: ConsumerContext + 'static = DefaultConsumerContext> {
    consumer: BaseConsumer<C>,
    queue_cb_arg: Pin<Box<QueueNonEmptyCbArg>>,
}

impl<C: ConsumerContext> Consumer<C> for StreamConsumer<C> {
    fn get_base_consumer(&self) -> &BaseConsumer<C> {
        return &self.consumer;
    }
}

impl FromClientConfig for StreamConsumer {
    fn from_config(config: &ClientConfig) -> KafkaResult<StreamConsumer> {
        StreamConsumer::from_config_and_context(config, DefaultConsumerContext)
    }
}

/// Native message queue nonempty callback. This callback will run whenever the
/// consumer's message queue switches from empty to nonempty.
extern "C" fn native_message_queue_nonempty_cb(
    _: *mut RDKafka,
    opaque_ptr: *mut c_void,
) {
    
    // restore opaque pointer into Pin<&T>. We could restore into &T, but
    // keeping Pin<> protects us from accidental moving out
    // Original pointer was for Pin<Box<T>>, but Box<T> owns value
    // and frees it in Drop, thats not what we want.
    let queue_arg : Pin<&QueueNonEmptyCbArg>;
    unsafe {
        queue_arg = Pin::new_unchecked(&*(opaque_ptr as *mut QueueNonEmptyCbArg));
    }

    queue_arg.waker.lock().unwrap().take().map(|w| w.wake());
    return ();
}

/// Creates a new `StreamConsumer` starting from a `ClientConfig`.
impl<C: ConsumerContext> FromClientConfigAndContext<C> for StreamConsumer<C> {
    fn from_config_and_context(
        config: &ClientConfig,
        context: C,
    ) -> KafkaResult<StreamConsumer<C>> {
        let stream_consumer = StreamConsumer {
            consumer: BaseConsumer::from_config_and_context(config, context)?,
            queue_cb_arg: QueueNonEmptyCbArg::new(),
        };

        let client = stream_consumer.client();
        let queue = client.consumer_queue().unwrap();
        let arg: *const QueueNonEmptyCbArg = &*stream_consumer.queue_cb_arg;

        unsafe {
            rdsys::rd_kafka_queue_cb_event_enable(
                queue.ptr(),
                Some(native_message_queue_nonempty_cb),
                arg as *mut c_void,
            );
        }
        Ok(stream_consumer)
    }
}

impl<C: ConsumerContext> StreamConsumer<C> {}

impl<C: ConsumerContext> Drop for StreamConsumer<C> {
    fn drop(&mut self) {
        trace!("Destroy StreamConsumer");
    }
}

impl<'a, C: ConsumerContext> Stream for &'a StreamConsumer<C> {
    type Item = KafkaResult<BorrowedMessage<'a>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        if let Some(m_ptr) = self
            .consumer
            .poll_raw(Timeout::After(Duration::from_secs(0)))
        {
            let msg = unsafe { BorrowedMessage::from_consumer(m_ptr, &self.consumer) };
            return Poll::Ready(Some(msg));
        }

        // Queue is empty, need to arrange our awakening
        {
            let mut waker = self.queue_cb_arg.waker.lock().unwrap();
            *waker = Some(cx.waker().clone());
        }

        // While doing previous step, it is possible that old `self.waker` was
        // notified, we don't want to miss this notification
        // so we poll again

        match self
            .consumer
            .poll_raw(Timeout::After(Duration::from_secs(0)))
        {
            None => {
                // Nope, still nothing, returning Pening as planned
                return Poll::Pending;
            }
            Some(m_ptr) => {
                // There was indeed a notification race, which we've caught
                let msg = unsafe { BorrowedMessage::from_consumer(m_ptr, &self.consumer) };
                return Poll::Ready(Some(msg));
            }
        };
    }
}
