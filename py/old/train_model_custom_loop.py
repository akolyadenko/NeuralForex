import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
import numpy as np

ds = tf.data.TFRecordDataset('/usr/proj/trd/train.tfrecord')

feature_description = {
    'year': tf.io.FixedLenFeature([], tf.int64, default_value=0),
    'month': tf.io.FixedLenFeature([], tf.int64, default_value=0),
    'day': tf.io.FixedLenFeature([], tf.int64, default_value=0),
    'hour': tf.io.FixedLenFeature([], tf.int64, default_value=0),
    'minute': tf.io.FixedLenFeature([], tf.int64, default_value=0),
    'profit_loss_histogram': tf.io.VarLenFeature(tf.float32),
}

def add_timeframe_feature_descriptions(type):
    feature_description[type + 'open'] = tf.io.VarLenFeature(tf.float32)
    feature_description[type + 'high'] = tf.io.VarLenFeature(tf.float32)
    feature_description[type + 'low'] = tf.io.VarLenFeature(tf.float32)
    feature_description[type + 'close'] = tf.io.VarLenFeature(tf.float32)
    feature_description[type + 'volume'] = tf.io.VarLenFeature(tf.float32)

add_timeframe_feature_descriptions('m1')
add_timeframe_feature_descriptions('m5')
add_timeframe_feature_descriptions('m15')
add_timeframe_feature_descriptions('m30')
add_timeframe_feature_descriptions('h1')
add_timeframe_feature_descriptions('h4')
add_timeframe_feature_descriptions('nextm1')

def _parse_function(example_proto):
    return tf.io.parse_single_example(example_proto, feature_description)
ds = ds.map(_parse_function)
ds = ds.batch(64)

feature_names = []
for tfm in ['m1', 'm5', 'm15', 'm30', 'h1', 'h4', ]:
    for f in ['open', 'high', 'low', 'close', 'volume']:
        feature_names.append(tfm + f)
# feature_names = ['m1open', 'm1high', 'm1low', 'm1close', 'm1volume',
#                 # 'm5open', 'm5high', 'm5low', 'm5close', 'm5volume',
#                 # 'm15open', 'm15high', 'm15low', 'm15close', 'm15volume',
#                 # 'm30open', 'm30high', 'm30low', 'm30close', 'm30volume',
#                 ]

inputs = []
for feature_name in feature_names:
    inputs.append(keras.Input(shape=(60,)))
input = layers.Concatenate(axis=1)(inputs)
net = layers.Dense(2048, activation="relu")(input)
for i in range(8):
    net = layers.Dense(2048, activation="relu")(net)
outputs = layers.Dense(100, name="predictions", activation='softmax')(net)
model = keras.Model(inputs=inputs, outputs=outputs)
model.summary()

optimizer = keras.optimizers.SGD(learning_rate=1e-1)
# loss_fn = keras.losses.MeanSquaredError()

def build_features_vector(x):
    l = []
    for feature_name in feature_names:
        # l.append(tf.sparse.to_dense(x[tfm + f]))
        l.append(x[feature_name])
    return l

step = 0
while True:
    for x in ds:
        with tf.GradientTape() as tape:
            # print(x['m1open'])
            logits = model(build_features_vector(x), training=True)
            # logits = model([x['m1open']], training=True)
            # labels = build_labels(logits, x)
            # print('logits = ' + str(logits))
            profit_loss_histogram = tf.sparse.to_dense(x['profit_loss_histogram'])
            loss_value = tf.reduce_sum(tf.math.multiply(profit_loss_histogram, logits))
            tf.print('loss_value:', loss_value)
            # tf.print('logits:', logits, summarize=-1)
            # tf.print('profit_loss_histogram:', profit_loss_histogram)

        grads = tape.gradient(loss_value, model.trainable_weights)
        optimizer.apply_gradients(zip(grads, model.trainable_weights))
        step += 1
        print('step ' + str(step))
        if step % 1000 == 0:
            model.save('/usr/proj/trd/model/step_' + str(step) + '/')


