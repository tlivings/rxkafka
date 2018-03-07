'use strict';

const { Observable } = require('rxjs');
const debuglog = require('debug');

const debug = debuglog('rxkafka-ConsumerReplaySubscription');

class ConsumerReplaySubscription extends Observable {
    constructor(consumer, { topic, offset, partition, timeout = 500, keepOpen = false }) {
        super((observer) => {
            debug('Subscribing');

            let lowOffset, highOffset;

            consumer.queryWatermarkOffsets(topic, partition, timeout, (error, offsets) => {
                if (error) {
                    debug('Error retrieving offsets');
                    observer.error(error);
                    return;
                }

                debug(`Offsets: high ${offsets.highOffset}, low ${offsets.lowOffset}`);

                lowOffset = offsets.lowOffset;
                highOffset = offsets.highOffset;

                if (offset >= highOffset) {
                    observer.complete();
                    return;
                }

                consumer.assign([{ topic, offset, partition }]);
                consumer.consume();
            });

            consumer.on('event.error', (error) => {
                debug(`Error: ${error.message}`);
                observer.error(error);
            });

            consumer.on('data', (data) => {
                debug(`Received data (offset ${data.offset}, highOffset ${highOffset})`);
                observer.next(data);
                if (!keepOpen && data.offset === highOffset - 1) {
                    debug('Reached highOffset and keepOpen = false');
                    observer.complete();
                }
            });

            return function dispose() {
                consumer.disconnect();
                debug('Disposed.');
            };
        });
    }
}

module.exports = ConsumerReplaySubscription;
