'use strict';

const { Observable } = require('rxjs');
const debuglog = require('debug');

const debug = debuglog('rxkafka-ConsumerConnection');

class ConsumerConnection extends Observable {
    constructor(consumer, { timeout = 1000 } = {}) {
        super((observer) => {
            debug('subscribing');

            consumer.connect({ timeout });

            const onFail = () => {
                debug('Connect failure');
                observer.error(new Error('Connection Error'));
            };

            const onReady = (info) => {
                debug('Connected:', info);
                observer.next(consumer);
                observer.complete();
            };

            const onDisconnect = () => {
                debug('Disconnected');
            };

            consumer.on('connection.failure', onFail);
            consumer.on('ready', onReady);
            consumer.on('disconnected', onDisconnect);

            return function dispose() {
                debug('Disposed');
                if (consumer.isConnected) {
                    consumer.disconnect();
                }
                consumer.removeListener('connection.failure', onFail);
                consumer.removeListener('ready', onReady);
                consumer.removeListener('disconnected', onDisconnect);
            };
        });

        this._consumer = consumer;
    }

    get consumer() {
        return this._consumer;
    }
}


module.exports = ConsumerConnection;
