'use strict';

const { Subject, AnonymousSubject } = require('rxjs');
const debuglog = require('debug');

const debug = debuglog('rxkafka-ProducerSubject');

class ProducerSubject extends AnonymousSubject {
    constructor(connection) {
        const recast = new Subject();

        super({
            next: ({ topic, partition, value, key }) => {
                debug(`Writing to topic ${topic}`);
                recast.next({ topic, partition, value, key });
                this._producer.produce(topic, partition, value, key);
            },

            error: (error) => {
                debug(`Error: ${error.message}`);
                recast.error(error);
                this._producer.emit('error', error);
            },

            complete: () => {
                this._producer.flush(500, () => {
                    debug('Flushed');
                    this._producer.disconnect();
                    recast.complete();
                    debug('Complete');
                });
            }
        }, recast);

        this._connection = connection;
        this._producer = connection.producer;
    }

    get connection() {
        return this._connection;
    }

    get producer() {
        return this._producer;
    }
}

module.exports = ProducerSubject;
