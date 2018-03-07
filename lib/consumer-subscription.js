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

            consumer.on('event.error', (error) => {
                debug(`Error: ${error.message}`);
                observer.error(error);
            });

            consumer.on('data', (data) => {
                debug(`Received data`);
                observer.next(data);
            });

            consumer.on('disconnected', () => {
                debug('Complete');
                observer.complete();
            });

            return function dispose() {
                consumer.disconnect();
                debug('Disposed');
            };
        });
    }
}

module.exports = ConsumerSubscription;
