'use strict';

const { Observable } = require('rxjs');
const debuglog = require('debug');

const debug = debuglog('rxkafka-ConsumerSubscription');

class ConsumerSubscription extends Observable {
    constructor(consumer, topics) {
        super((observer) => {
            debug('Subscribing');

            consumer.subscribe(topics);
            consumer.consume();

            const onError = (error) => {
                debug(`Error: ${error.message}`);
                observer.error(error);
            };

            const onData = (data) => {
                debug(`Received data`);
                observer.next(data);
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

module.exports = ConsumerSubscription;
