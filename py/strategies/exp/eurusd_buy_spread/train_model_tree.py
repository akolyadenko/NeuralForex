# python3 ../py/strategies/exp/eurusd_buy/train_model.py
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
import data
import render_predictions_lib

batch_size = 256

inputs = {}
l = []
HIDDEN_SIZE = 128

# for f in data.timeframe_features:
#    input = keras.Input(shape=(240,), dtype=tf.float32, name=f)
#    inputs[f] = input
#    l.append(input)

# for t in ['m1', 'm5']:
#    ll = []
#    for i in ['open', 'close', 'high', 'low']:
#        f = t + i + '_rescaled'
#        input = keras.Input(shape=(240,), dtype=tf.float32, name=f)
#        inputs[f] = input
#        input = layers.Reshape((240,1))(input)
#        ll.append(input)
#    net = layers.concatenate(ll, 2)
#    net = layers.Dropout(0.2)(net)
#    net = layers.Dense(8, activation='relu')(net)
#    net = layers.Conv1D(15, 8, activation='sigmoid')(net)
#    net = layers.Flatten()(net)
#    l.append(net)

def build_tree(net, size):
    if size > 15:
        bucket_size = int(size / 2)
        # t1 = tf.slice(net, begin=[0, 0, 0], size=[None, bucket_size, 32])
        # t2 = tf.slice(net, begin=[0, bucket_size, 0], size=[None, bucket_size, 32])
        t1 = tf.slice(net, begin=[0, 0, 0], size=[batch_size, bucket_size, HIDDEN_SIZE])
        t2 = tf.slice(net, begin=[0, bucket_size, 0], size=[batch_size, bucket_size, HIDDEN_SIZE])
        net1 = build_tree(t1, bucket_size)
        net2 = build_tree(t2, bucket_size)
        l = [net1, net2]
        net = tf.concat(l, 1)
        net = layers.Dense(HIDDEN_SIZE, activation='relu')(net)
        net = layers.Dense(HIDDEN_SIZE, activation='relu')(net)
        # net = layers.Dropout(0.2)(net)
        return net
    else:
        net = layers.Flatten()(net)
        net = layers.Dense(HIDDEN_SIZE, activation='relu')(net)
        net = layers.Dense(HIDDEN_SIZE, activation='relu')(net)
        # net = layers.Dropout(0.2)(net)
        return net

for t in ['m1', 'm5']:
    ll = []
    for i in ['open', 'close', 'high', 'low']:
        f = t + i + '_rescaled'
        input = keras.Input(shape=(240,), dtype=tf.float32, name=f)
        inputs[f] = input
        input = layers.Reshape((240,1))(input)
        ll.append(input)
    net = layers.concatenate(ll, 2)
    # net = layers.Dropout(0.2)(net)
    net = layers.Dense(HIDDEN_SIZE, activation='relu')(net)
    net = tf.reshape(net, [batch_size, 240, HIDDEN_SIZE])
    # net = layers.Conv1D(10, 24, activation='relu')(net)
    net = build_tree(net, 240)
    l.append(net)


net = layers.concatenate(l, 1)
# net = layers.Dropout(0.2)(net)
# for i in range(2):
#    net = layers.Dense(1024, activation='relu')(net)
#    net = layers.Dropout(0.1)(net)
net = layers.Flatten()(net)
net = layers.Dense(20, activation='relu')(net)
net = layers.Dense(2, activation='relu')(net)
net = tf.where(
    tf.math.greater(net, tf.constant(1.0, shape=net.shape)),
    tf.constant(1.0, shape=net.shape),
    net)
model = keras.Model(inputs=inputs, outputs=net)

model.summary()

min_val_loss = 100000.0
min_val_loss_epoch = -1
class LossAndErrorPrintingCallback(tf.keras.callbacks.Callback):
    def on_epoch_end(self, epoch, logs=None):
        global min_val_loss, min_val_loss_epoch
        val_loss = logs["val_loss"]
        if val_loss < min_val_loss:
            min_val_loss = val_loss
            min_val_loss_epoch = epoch
        print(" Eval loss: {:7.8f} ".format(logs["val_loss"]))

# model = keras.models.load_model('/usr/proj/trd/model')

def compile_and_fit(epochs, rate):
    model.compile(
        loss=tf.keras.losses.MeanSquaredError(),
        optimizer=keras.optimizers.Adam(learning_rate=rate),
        metrics=[tf.keras.metrics.TruePositives(thresholds=0.8), tf.keras.metrics.FalsePositives(thresholds=0.8),
                tf.keras.metrics.Precision(thresholds=0.8), tf.keras.metrics.Recall(thresholds=0.8)])

    model.fit(
        data.create_ds('train', batch_size),
        epochs=epochs,
        validation_data=data.create_ds('test', batch_size),
        callbacks=[LossAndErrorPrintingCallback()])

compile_and_fit(20, 1e-4)
# compile_and_fit(5, 1e-5)

print('min_val_loss = ' + str(min_val_loss))
print('min_val_loss_epoch = ' + str(min_val_loss_epoch))

for (x, y) in data.create_ds('test', batch_size):
    break
    predictions = model.predict(x)
    count = 0
    print('x = ' + str(x))
    print('y = ' + str(y))
    for p in predictions:
        print('predictions: ' + str(p))
        count += 1
        if (count == 10):
            break
    break

model_path = '/usr/proj/trd/models/exp/base/1/'
model.save(model_path)

render_predictions_lib.render_predictions(model_path,batch_size)
