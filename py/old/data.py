import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
import random

def create_tfrecord_ds(split, batch_size, for_prediction=False):
    ds = tf.data.TFRecordDataset('/usr/proj/trd/' + split + '.tfrecord')
    feature_description = {
        'epoch_min': tf.io.FixedLenFeature((), tf.int64),
        'profit_loss_histogram': tf.io.FixedLenFeature([100], tf.float32),
    }

    timeframe_feature_names = []
    for timeframe in ['m1', 'm5']:
        for feature in ['open', 'close', 'high', 'low']:
            timeframe_feature_names.append(timeframe + feature + '_rescaled')
    for f in timeframe_feature_names:
        feature_description[f] = tf.io.FixedLenFeature([60], tf.float32)

    def _parse_function(example_proto):
        return tf.io.parse_single_example(example_proto, feature_description)
    ds = ds.map(_parse_function)
    # ds = ds.map(lambda x: (
    #    tf.convert_to_tensor([random.randint(0,10) for i in range(60)], dtype_hint=tf.float32),
    #    tf.convert_to_tensor([1] * 20 + [0]*80, dtype_hint=tf.float32)))
    def _map_values(x):
        if for_prediction:
            label = x['epoch_min']
        else:
            label = x['profit_loss_histogram']

        l = []
        for f in timeframe_feature_names:
            l.append(x[f])
        return (tf.concat(l, 0), label)

    ds = ds.map(lambda x: _map_values(x))
    return ds

def create_ds(split, batch_size, for_prediction=False):
    ds = create_tfrecord_ds(split, batch_size, for_prediction)
    ds = ds.batch(batch_size, drop_remainder=False)
    ds = ds.cache()
    return ds

def create_py_test_tfrecord_file():
    writer = tf.io.TFRecordWriter('/usr/proj/trd/py_test.tfrecord')
    for i in range(100000):
        example = tf.train.Example(features = tf.train.Features(feature = {
            'm1open_rescaled': tf.train.Feature(float_list = tf.train.FloatList(
                value=[random.randint(750,770) for i in range(60)])),
            'profit_loss_histogram': tf.train.Feature(float_list = tf.train.FloatList(
                value=[1]+[random.randint(0,1) for i in range(99)])),
            # 'profit_loss_histogram': tf.train.Feature(float_list = tf.train.FloatList(
            #    value=[1]*20+[0]*80)),
        }))
        writer.write(example.SerializeToString())
    writer.close()

# create_py_test_tfrecord_file()

def create_rand_ds():
    def _random_generator():
        for i in range(100000):
            yield ([random.randint(750,770) for i in range(60)], [1]*20+[0]*80)

    ds = tf.data.Dataset.from_generator(_random_generator,
                                        (tf.float32, tf.float32),
                                        ((60), (100)))
    # ds = ds.map(lambda x, y: (
    #    tf.convert_to_tensor(
    #        tf.constant([random.randint(0,10) for i in range(60)], dtype=tf.float32),
    #        dtype_hint=tf.float32),
    #    tf.convert_to_tensor(
    #        tf.constant([1] * 20 + [0]*80, dtype=tf.float32), dtype_hint=tf.float32)))
    return ds