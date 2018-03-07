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

            const onError = (error) => {
                debug(`Error: ${error.message}`);
                observer.error(error);
            };

            const onData = (data) => {
                debug(`Received data (offset ${data.offset}, highOffset ${highOffset})`);
                observer.next(data);
                if (!keepOpen && data.offset === highOffset - 1) {
                    debug('Reached highOffset and keepOpen = false');
                    observer.complete();
                }
            };

            const onDisconnect = () => {
                debug('Complete');
                observer.complete();
            };

            consumer.on('event.error', onError);
            consumer.on('data', onData);
            consumer.on('disconnected', onDisconnect);

            return function dispose() {
                consumer.disconnect();
                consumer.removeListener('event.error', onError);
                consumer.removeListener('data', onData);
                consumer.removeListener('disconnect', onDisconnect);
                debug('Disposed');
            };
        });
    }
}

module.exports = ConsumerReplaySubscription;
