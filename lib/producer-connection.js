'use strict';

const { Observable } = require('rxjs');
const debuglog = require('debug');

const debug = debuglog('rxkafka-ProducerConnection');

class ProducerConnection extends Observable {
    constructor(producer) {

        super((observer) => {
            debug('Subscribing');

            producer.connect({ timeout: 500 });

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

            producer.on('connection.failure', onFail);
            producer.on('ready', onReady);
            producer.on('disconnected', onDisconnect);

            return function dispose() {
                debug('Disposed');
                producer.removeListener('connection.failure', onFail);
                producer.removeListener('ready', onReady);
                producer.removeListener('disconnected', onDisconnect);
            };
        });

        this._producer = producer;
    }

    get producer() {
        return this._producer;
    }
}

module.exports = ProducerConnection;
