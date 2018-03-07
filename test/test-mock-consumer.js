'use strict';

const Test = require('tape');
const { ConsumerConnection, ConsumerSubscription, ConsumerReplaySubscription } = require('../lib');
const MockConsumer = require('./fixtures/mock-consumer');
const { Observable } = require('rxjs');

Test('test connections', (t) => {

    t.test('successful connection', (t) => {
        t.plan(2);

        const consumer = new MockConsumer({
            'metadata.broker.list': 'localhost:9092',
            'group.id': 'mymirror',
            event_cb: true
        });

        new ConsumerConnection(consumer).subscribe(
            (connection) => {
                t.ok(connection, 'got connection');
            },
            (error) => {
                t.fail(error.message);
            },
            () => {
                t.pass('complete');
            }
        );

    });

    t.test('error in connection', (t) => {
        t.plan(1);

        const consumer = new MockConsumer({
            'metadata.broker.list': 'localhost:9092',
            'group.id': 'mymirror',
            event_cb: true
        });

        consumer.errorOn = {
            'connect': true
        };

        new ConsumerConnection(consumer).subscribe(
            (connection) => {
                t.fail('got connection');
            },
            (error) => {
                t.pass('received error');
            },
            () => {
                t.fail('complete');
            }
        );

    });

});

Test.skip('test data', (t) => {

    t.test('successful consume', (t) => {
        t.plan(2);


        const consumer = new MockConsumer({
            'metadata.broker.list': 'localhost:9092',
            'group.id': 'mymirror',
            event_cb: true
        });

        consumer.dataProducer = function () {
            return {};
        };


        new ConsumerConnection(consumer).concatMap((consumer) => {
            return new ConsumerSubscription(consumer, ['my-topic']);
        })
        .take(1)
        .subscribe(
            (data) => {
                t.ok(data, 'got data');
                consumer.disconnect();
            },
            (error) => {
                t.error(error);
            },
            () => {
                t.pass('complete');
            }
        );

    });

    t.test('successful replay', (t) => {

        const consumer = new MockConsumer({
            'metadata.broker.list': 'localhost:9092',
            'group.id': 'mymirror',
            event_cb: true
        });

        consumer.dataProducer = function () {
            return {};
        };


        new ConsumerConnection(consumer).concatMap((consumer) => {
            return new ConsumerReplayStream(consumer, { topic: 'my-topic' });
        })
        .subscribe(
            (data) => {
                t.ok(data, 'got data');
            },
            (error) => {
                t.error(error);
            },
            () => {
                t.pass('complete');
                t.end();
            }
        );

    });

    t.test('successful replay from later offset', (t) => {
        t.plan(3);

        const consumer = new MockConsumer({
            'metadata.broker.list': 'localhost:9092',
            'group.id': 'mymirror',
            event_cb: true
        });

        consumer.dataProducer = function () {
            return {};
        };


        new ConsumerConnection(consumer).concatMap((consumer) => {
            return new ConsumerReplayStream(consumer, { topic: 'my-topic', offset: 3 });
        })
        .subscribe(
            (data) => {
                t.ok(data, 'got data');
            },
            (error) => {
                t.error(error);
            },
            () => {
                t.pass('complete');
            }
        );

    });

});
