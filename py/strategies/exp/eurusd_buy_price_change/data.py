import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
import random

histogram_mins = ['5', '15', '30', '60', '120', '240']
indicators = ['open', 'close', 'high', 'low']
timeframe_features = []
for timeframe in ['m1', 'm5', 'm15', 'm30']:
    for feature in ['open', 'close', 'high', 'low']:
        timeframe_features.append(timeframe + feature + '_rescaled')

range_features = []
for timeframe in ['m1', 'm5', 'm15', 'm30']:
    for feature in ['rangeHigh', 'rangeLow',
                    'rangeHighLinearA', 'rangeHighLinearB', 'rangeLowLinearA', 'rangeLowLinearB']:
        range_features.append(timeframe + feature + '_rescaled')
    for feature in ['rangeHighProximity5pCounts', 'rangeHighProximity10pCounts', 'rangeHighProximity20pCounts',
                    'rangeLowProximity5pCounts', 'rangeLowProximity10pCounts', 'rangeLowProximity20pCounts']:
        range_features.append(timeframe + feature + '_rescaled')

# timeframe_features_original = []
# for timeframe in ['m1', 'm5', 'm15', 'm30']:
#    for feature in ['open', 'close', 'high', 'low']:
#        timeframe_features_original.append(timeframe + feature)#

def create_tfrecord_ds(split, batch_size, for_prediction=False):
    ds = tf.data.TFRecordDataset('/usr/proj/trd/' + split + '.tfrecord')
    feature_description = {
        'epoch_min': tf.io.FixedLenFeature((), tf.int64),
        'hour': tf.io.FixedLenFeature((), tf.int64),
        'day_of_week': tf.io.FixedLenFeature((), tf.int64),
        'next_tfm_highs': tf.io.FixedLenFeature([24], tf.float32),
        'next_tfm_lows': tf.io.FixedLenFeature([24], tf.float32),
    }

    for mins in histogram_mins:
        feature_description['price_decrease_histogram_' + mins] = tf.io.FixedLenFeature([100], tf.float32)
        feature_description['price_increase_histogram_' + mins] = tf.io.FixedLenFeature([100], tf.float32)
        # feature_description['price_change_min_' + mins] = tf.io.FixedLenFeature([1], tf.float32)
        feature_description['price_change_max_' + mins] = tf.io.FixedLenFeature([1], tf.float32)
        feature_description['price_change_avg_' + mins] = tf.io.FixedLenFeature([1], tf.float32)

    for f in timeframe_features:
        feature_description[f] = tf.io.FixedLenFeature([60], tf.float32)

    for f in range_features:
        feature_description[f] = tf.io.FixedLenFeature([6], tf.float32)

    # for f in timeframe_features_original:
    #    feature_description[f] = tf.io.FixedLenFeature([60], tf.float32)

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
            l = []
            for tfm in ['30', '60', '120', '240']:
                for i in range(20):
                    l.append([x['price_increase_histogram_' + tfm][i]])
            label = tf.concat(l, 0)
            #label = tf.concat([
                #x['price_change_max_240'][0:1],
                #x['price_change_max_240'][0:1]],
            #    x['price_increase_histogram_240'][7:8],
            #    x['price_increase_histogram_60'][4:5]],
            #                  0)
            # tf.print('labels = ', label)

        l = []
        for f in timeframe_features:
            l.append(x[f])
        features = {}
        features['features_concat'] = tf.concat(l, 0)
        for f in timeframe_features:
            features[f] = x[f]

        for f in range_features:
            features[f] = x[f]
        # for f in timeframe_features_original:
        #    features[f] = x[f]
        features['hour'] = x['hour']
        features['day_of_week'] = x['day_of_week']
        return (features, label)

    ds = ds.map(lambda x: _map_values(x))
    return ds

def create_ds(split, batch_size, for_prediction=False):
    ds = create_tfrecord_ds(split, batch_size, for_prediction)
    # ds = ds.cache('/usr/proj/trd/tmp')
    if not for_prediction:
        ds = ds.shuffle(buffer_size=50000)
    ds = ds.batch(batch_size, drop_remainder=True)
    ds = ds.prefetch(buffer_size=50000)
    return ds
