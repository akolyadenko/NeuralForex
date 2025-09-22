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
HIDDEN_SIZE = 4
DROPOUT_RATE = 0.2
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
    for type in ['bid', 'ask']:
        for i in ['high', 'low']:
            f = t + type + i + '_rescaled'
            input = keras.Input(shape=(60,), dtype=tf.float32, name=f)
            inputs[f] = input
            input = layers.Dropout(DROPOUT_RATE)(input)
            input = layers.Reshape((60, 1))(input)
            ll.append(input)
    net = layers.concatenate(ll, 2)
    print('create_timeframe_subnet after concat net.shape = '
          + str(net.shape))
    # net = tf.reshape(net, [batch_size, 60, 2])
    # net = layers.Dense(HIDDEN_SIZE, activation='gelu')(net)
    # net = layers.Dense(HIDDEN_SIZE, activation='gelu')(net)
    net = net + keras_nlp.layers.PositionEmbedding(60)(net)
    # net = layers.Flatten()(net)
    for i in range(8):
        net = layers.Dense(HIDDEN_SIZE, activation='gelu')(net)
    return net

def create_timeframe_discrete_subnet(t):
    ll = []
    for type in ['bid', 'ask']:
        for i in ['high', 'low']:
            f = t + type + i + '_rescaled_discrete'
            input = keras.Input(shape=(60,), dtype=tf.int64, name=f)
            inputs[f] = input
            input = layers.Reshape((60, 1))(input)
            ll.append(input)
    net = layers.concatenate(ll, 2)
    print('create_timeframe_discrete_subnet after concat net.shape = '
          + str(net.shape))
    net = tf.one_hot(net, 40, dtype=tf.float32)
    net = layers.Dropout(DROPOUT_RATE)(net)
    print('create_timeframe_discrete_subnet after one_hot net.shape = '
          + str(net.shape))
    for i in range(2):
        net = layers.Dense(HIDDEN_SIZE, activation='gelu')(net)
    net = layers.Reshape((60, 4*HIDDEN_SIZE))(net)

    print('create_timeframe_discrete_subnet after flatten net.shape = '
          + str(net.shape))
    # net = tf.reshape(net, [batch_size, 60, 2])
    # net = layers.Dense(HIDDEN_SIZE, activation='gelu')(net)
    # net = layers.Dense(HIDDEN_SIZE, activation='gelu')(net)
    # net = layers.Flatten()(net)
    for i in range(2):
        net = layers.Dense(HIDDEN_SIZE, activation='gelu')(net)
    return net

hour = keras.Input(shape=(), dtype=tf.int64, name='hour')
inputs['hour'] = hour
hour = layers.Dense(HIDDEN_SIZE, activation='gelu')(tf.one_hot(hour, 24))
hour = tf.expand_dims(hour, 1)
day_of_week = keras.Input(shape=(), dtype=tf.int64, name='day_of_week')
inputs['day_of_week'] = day_of_week
day_of_week = layers.Dense(HIDDEN_SIZE, activation='gelu')(tf.one_hot(day_of_week, 7))
day_of_week = tf.expand_dims(day_of_week, 1)
current_bid_rescaled = keras.Input(shape=(), dtype=tf.float32, name='current_bid_rescaled')
inputs['current_bid_rescaled'] = current_bid_rescaled
current_bid_rescaled = tf.expand_dims(current_bid_rescaled, 1)
current_bid_rescaled = layers.Dense(HIDDEN_SIZE, activation='gelu')(current_bid_rescaled)
current_bid_rescaled = tf.expand_dims(current_bid_rescaled, 1)

def create_last_timepoint_subnet():
    ll = []
    for f in data.range_features:
        input = keras.Input(shape=(6,), dtype=tf.float32, name=f)
        inputs[f] = input
        input = layers.Dropout(DROPOUT_RATE)(input)
        input = layers.Reshape((6, 1))(input)
        ll.append(input)
    net = layers.concatenate(ll, 2)
    for i in range(2):
        net = layers.Dense(HIDDEN_SIZE, activation='gelu')(net)
    return net

def create_last_timepoint_discrete_subnet():
    ll = []
    for f in data.range_discrete_features:
        input = keras.Input(shape=(6,), dtype=tf.int64, name=f)
        inputs[f] = input
        input = layers.Reshape((6, 1))(input)
        ll.append(input)
    net = layers.concatenate(ll, 2)
    net = tf.one_hot(net, 40, dtype=tf.float32)
    net = layers.Dropout(DROPOUT_RATE)(net)
    for i in range(2):
        net = layers.Dense(HIDDEN_SIZE, activation='gelu')(net)
    net = layers.Reshape((6, 32*HIDDEN_SIZE))(net)
    for i in range(2):
        net = layers.Dense(HIDDEN_SIZE, activation='gelu')(net)
    return net

net = tf.concat([
    current_bid_rescaled,
    create_last_timepoint_subnet(),
    create_last_timepoint_discrete_subnet(),
    hour,
    day_of_week,
    # create_timeframe_discrete_subnet('m5'),
    # create_timeframe_discrete_subnet('m15'),
    # create_timeframe_subnet('m5'),
    # create_timeframe_subnet('m15'),
    # create_timeframe_subnet('m1'),
    # create_timeframe_subnet('m30'),
    ], 1)

# net = create_last_timepoint_subnet()



# net = hnode(net)
net = layers.Flatten()(net)
for i in range(8):
    net = hnode(net)
net = layers.Flatten()(net)

net = layers.Dense(HIDDEN_SIZE, activation='gelu')(net)
net = layers.Flatten()(net)
net = layers.Dense(80, activation='gelu')(net)
net = tf.minimum(net, tf.constant(1.0))
net = tf.maximum(net, tf.constant(0.0))
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
            render_predictions_lib.render_for_split('eval1', batch_size, saved_checkpoint, model)
            render_predictions_lib.render_for_split('eval2', batch_size, saved_checkpoint, model)
            render_predictions_lib.render_for_split('eval3', batch_size, saved_checkpoint, model)
            render_predictions_lib.render_for_split('eval4', batch_size, saved_checkpoint, model)
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

compile_and_fit(100, 1e-4, train_ds, test_ds)

print('min_val_loss = ' + str(min_val_loss))
print('min_val_loss_epoch = ' + str(min_val_loss_epoch))

model.save(model_path + str(saved_checkpoint))

# render_predictions_lib.render_predictions(model_path,batch_size, saved_checkpoint)
