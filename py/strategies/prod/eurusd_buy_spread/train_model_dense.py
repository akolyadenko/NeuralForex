# python3 ../py/strategies/exp/eurusd_buy/train_model.py
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
import data
import render_predictions_lib
import datetime
import keras_nlp

batch_size = 256

inputs = {}
HIDDEN_SIZE = 16
model_path = '/usr/proj/trd/models/exp/base/'
render_predictions_lib.create_schema()

l = []

timeframe_subnet_dense = layers.Dense(HIDDEN_SIZE, activation='gelu')

shared_hnode_dense_emb_1 = layers.Dense(HIDDEN_SIZE, activation='gelu')
shared_hnode_dense_emb_2 = layers.Dense(HIDDEN_SIZE, activation='gelu')

def hnode(net):
    net1 = layers.Dense(HIDDEN_SIZE, activation='gelu')(net)
    net2 = layers.Dense(HIDDEN_SIZE, activation='sigmoid')(net)
    net3 = layers.Dense(HIDDEN_SIZE, activation='tanh')(net)
    net4 = layers.Dense(HIDDEN_SIZE, activation='swish')(net)
    net5 = layers.Dense(HIDDEN_SIZE, activation='relu')(net)
    net = tf.concat([net1, net2, net3, net4, net5], 1, name='concat_hnode')
    net = layers.Dense(HIDDEN_SIZE, activation='gelu')(net)
    net = layers.Dropout(0.2)(net)
    return net

def create_timeframe_subnet(t):
    ll = []
    # for i in ['open', 'close', 'high', 'low']:
    for i in ['high', 'low']:
        f = t + i + '_rescaled'
        input = keras.Input(shape=(60,), dtype=tf.float32, name=f)
        inputs[f] = input
        input = layers.Reshape((60,1))(input)
        ll.append(input)
    net = layers.concatenate(ll, 2)
    net = tf.reshape(net, [batch_size, 60, 2])
    net = layers.Dense(HIDDEN_SIZE, activation='gelu')(net)
    net = layers.Dense(HIDDEN_SIZE, activation='gelu')(net)
    net = net + keras_nlp.layers.PositionEmbedding(60)(net)
    net = layers.Dropout(0.2)(net)
    net = layers.Flatten()(net)
    for i in range(8):
        net = hnode(net)
    return net

hour = keras.Input(shape=(), dtype=tf.int64, name='hour')
inputs['hour'] = hour
hour = layers.Dense(4, activation='gelu')(tf.one_hot(hour, 24))
day_of_week = keras.Input(shape=(), dtype=tf.int64, name='day_of_week')
inputs['day_of_week'] = day_of_week
day_of_week = layers.Dense(4, activation='gelu')(tf.one_hot(day_of_week, 7))

def create_last_timepoint_subnet():
    ll = []
    for f in data.range_features:
        input = keras.Input(shape=(6,), dtype=tf.float32, name=f)
        inputs[f] = input
        input = layers.Reshape((6,1))(input)
        ll.append(input)
    net = layers.concatenate(ll, 2)
    net = layers.Dense(HIDDEN_SIZE, activation='gelu')(net)
    net = layers.Dense(HIDDEN_SIZE, activation='gelu')(net)
    net = net + keras_nlp.layers.PositionEmbedding(8)(net)
    net = layers.Flatten()(net)
    for i in range(6):
        net = hnode(net)
    return net


net = tf.concat([
    # create_timeframe_subnet('m1'),
    create_timeframe_subnet('m5'),
    create_timeframe_subnet('m15'),
    create_timeframe_subnet('m30'),
    create_last_timepoint_subnet(),
    hour,
    day_of_week], 1)



net = hnode(net)
net = layers.Flatten()(net)
for i in range(8):
    net = hnode(net)
net = layers.Dense(80, activation='gelu')(net)
net = tf.minimum(net, tf.constant(1.0))
model = keras.Model(inputs=inputs, outputs=net)

model.summary()

min_val_loss = 100000.0
min_val_loss_epoch = -1
current_epoch = 0
saved_checkpoint = 0

class EpochCallback(tf.keras.callbacks.Callback):
    def on_epoch_end(self, epoch, logs=None):
        global min_val_loss, min_val_loss_epoch, current_epoch, saved_checkpoint
        val_loss = logs["val_loss"]
        if val_loss < min_val_loss:
            min_val_loss = val_loss
            min_val_loss_epoch = epoch
        print(" Eval loss: {:7.8f} ".format(logs["val_loss"]))
        if True:# current_epoch % 4 == 0:
            saved_checkpoint += 1
            print('Saving checkpoint #' + str(saved_checkpoint) + ' ' + str(datetime.datetime.now()))
            model.save(model_path + str(saved_checkpoint))
            print('Rendering predictions for checkpoint #' + str(saved_checkpoint) + ' '
                  + str(datetime.datetime.now()))
            render_predictions_lib.render_for_split('test', batch_size, saved_checkpoint, model)
            render_predictions_lib.render_for_split('eval', batch_size, saved_checkpoint, model)
            print('Completed checkpoint #' + str(saved_checkpoint) + ' ' + str(datetime.datetime.now()))
        current_epoch += 1



# model = keras.models.load_model('/usr/proj/trd/model')

def compile_and_fit(epochs, rate, train_ds, test_ds):
    model.compile(
        loss=tf.keras.losses.MeanSquaredError(),
        optimizer=keras.optimizers.Adam(learning_rate=rate))

    model.fit(
        train_ds,
        epochs=epochs,
        validation_data=test_ds,
        callbacks=[EpochCallback()])

train_ds = data.create_ds('train', batch_size)
test_ds = data.create_ds('test', batch_size)

compile_and_fit(50, 1e-4, train_ds, test_ds)
compile_and_fit(30, 1e-5, train_ds, test_ds)

print('min_val_loss = ' + str(min_val_loss))
print('min_val_loss_epoch = ' + str(min_val_loss_epoch))

model.save(model_path + str(saved_checkpoint))

# render_predictions_lib.render_predictions(model_path,batch_size, saved_checkpoint)
