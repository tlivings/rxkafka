'use strict';

const { EventEmitter } = require('events');

class MockConsumer extends EventEmitter {

    constructor(options, topicConfig, topicOptions, dataProducer, errorOn = {}) {
        super();
        this.options = options;
        this.topicConfig = topicConfig;
        this.topicOptions = topicOptions;
        this.dataProducer = dataProducer;
        this.errorOn = errorOn;
        this.startOffset = 0;
    }

    connect() {
        if (this.errorOn.connect) {
            setImmediate(() => this.emit('connection.failure', new Error()));
            return;
        }
        setImmediate(() => this.emit('ready', { name: 'rdkafka#consumer-1' }));
    }

    disconnect() {
        setImmediate(() => this.emit('disconnected'));
    }

    assign(options) {
        this.startOffset = options[0].offset || 0;
    }

    subscribe(...topics) {

    }

    consume() {
        for (let offset = this.startOffset; offset < MockConsumer.HIGH_OFFSET; offset++) {
            const data = this.dataProducer();
            setImmediate(() => {
                this.emit('data', Object.assign({ offset }, data));
            });
        }
    }

    queryWatermarkOffsets(topic, partition, timeout, cb) {
        if (this.errorOn.queryWatermarkOffsets) {
            setImmediate(() => cb(new Error()));
            return;
        }
        setImmediate(() => cb(null, { highOffset: MockConsumer.HIGH_OFFSET, lowOffset: 0 }));
    }

}

MockConsumer.HIGH_OFFSET = 5;

module.exports = MockConsumer;
