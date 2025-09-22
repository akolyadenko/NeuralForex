# python3 ../py/strategies/exp/eurusd_buy/train_model.py
import tensorflow as tf
from tensorflow import keras
from keras import backend as K
from tensorflow.keras import layers
import data
import render_predictions_lib

batch_size = 256

inputs = {}
l = []

for f in data.timeframe_features:
    input = keras.Input(shape=(240,), dtype=tf.float32, name=f)
    inputs[f] = input
    l.append(input)

net = layers.concatenate(l, 1)
net = layers.Dropout(0.2)(net)
for i in range(3):
    net = layers.Dense(1024, activation='relu')(net)
    net = layers.Dropout(0.1)(net)
net = layers.Flatten()(net)
net = layers.Dense(100, activation='relu')(net)
model = keras.Model(inputs=inputs, outputs=net)

model.summary()

# model.compile(loss=tf.keras.losses.MeanSquaredError(), optimizer=keras.optimizers.Adam(learning_rate=1e-4))

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

# model.fit(
#    data.create_ds('train', batch_size),
#    epochs=145,
#    validation_data=data.create_ds('test', batch_size),
#    callbacks=[LossAndErrorPrintingCallback()])

# loss_fn = tf.keras.losses.MeanSquaredError()
def loss_fn(yTrue,yPred):
    ones = K.ones_like(yTrue[0,:]) #a simple vector with ones shaped as (60,)
    idx = K.cumsum(ones) #similar to a 'range(1,61)'

    return K.mean(K.square(yTrue-yPred))

model.compile(loss=loss_fn, optimizer=keras.optimizers.Adam(learning_rate=1e-5))

model.fit(
  data.create_ds('train', batch_size),
  epochs=20,
  validation_data=data.create_ds('test', batch_size),
  callbacks=[LossAndErrorPrintingCallback()])

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

render_predictions_lib.render_predictions(model)
