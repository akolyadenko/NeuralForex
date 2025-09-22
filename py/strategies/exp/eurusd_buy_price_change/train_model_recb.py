# python3 ../py/strategies/exp/eurusd_buy/train_model.py
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
import data
import render_predictions_lib
import datetime

batch_size = 256

inputs = {}
HIDDEN_SIZE = 64
model_path = '/usr/proj/trd/models/exp/base/'
render_predictions_lib.create_schema()

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
#    net = layers.Dense(8, activation='gelu')(net)
#    net = layers.Conv1D(15, 8, activation='sigmoid')(net)
#    net = layers.Flatten()(net)
#    l.append(net)

shared_hnode_dense_subtree_1 = layers.Dense(HIDDEN_SIZE, activation='gelu')
shared_hnode_dense_subtree_2 = layers.Dense(HIDDEN_SIZE, activation='gelu')

shared_hnode_subtree_layers = []

for i in range(4):
    layer = {}
    layer['gelu'] = layers.Dense(HIDDEN_SIZE, activation='gelu')
    layer['relu'] = layers.Dense(HIDDEN_SIZE, activation='relu')
    layer['sigmoid'] = layers.Dense(HIDDEN_SIZE, activation='sigmoid')
    layer['tanh'] = layers.Dense(HIDDEN_SIZE, activation='tanh')
    layer['swish'] = layers.Dense(HIDDEN_SIZE, activation='swish')
    layer['dense'] = layers.Dense(HIDDEN_SIZE, activation='gelu')
    shared_hnode_subtree_layers.append(layer)

def hnode_subtree(net):
    net = shared_hnode_dense_subtree_1(net)
    net = shared_hnode_dense_subtree_2(net)

    for layer in shared_hnode_subtree_layers:
        net1 = layer['gelu'](net)
        net2 = layer['sigmoid'](net)
        net3 = layer['tanh'](net)
        net4 = layer['swish'](net)
        net5 = layer['relu'](net)
        net = tf.concat([net1, net2, net3, net4, net5], 1, name='concat_hnode')
        net = layer['dense'](net)

    return net

shared_hnode_dense_slice_1 = layers.Dense(HIDDEN_SIZE, activation='gelu')
shared_hnode_dense_slice_2 = layers.Dense(HIDDEN_SIZE, activation='gelu')

shared_hnode_slice_layers = []

for i in range(4):
    layer = {}
    layer['gelu'] = layers.Dense(HIDDEN_SIZE * 4, activation='gelu')
    layer['relu'] = layers.Dense(HIDDEN_SIZE * 4, activation='relu')
    layer['sigmoid'] = layers.Dense(HIDDEN_SIZE * 4, activation='sigmoid')
    layer['tanh'] = layers.Dense(HIDDEN_SIZE * 4, activation='tanh')
    layer['swish'] = layers.Dense(HIDDEN_SIZE * 4, activation='swish')
    layer['dense'] = layers.Dense(HIDDEN_SIZE, activation='gelu')
    shared_hnode_slice_layers.append(layer)

def hnode_slice(net):
    # net = layers.Dropout(0.5)(net)
    net = shared_hnode_dense_slice_1(net)
    net = shared_hnode_dense_slice_2(net)

    for layer in shared_hnode_slice_layers:
        net1 = layer['gelu'](net)
        net2 = layer['sigmoid'](net)
        net3 = layer['tanh'](net)
        net4 = layer['swish'](net)
        net5 = layer['relu'](net)
        net = tf.concat([net1, net2, net3, net4, net5], 1, name='concat_hnode')
        net = layer['dense'](net)

    return net

def create_slices_list(net):
    l = []
    i = 0
    slice_size = 10
    hidden_size = 4
    while i + slice_size <= 60:
        t = tf.slice(net, begin=[0, i, 0],
                     size=[batch_size, slice_size, hidden_size])
        l.append(t)
        i += 5
    return l

l = []

timeframe_subnet_dense = layers.Dense(HIDDEN_SIZE, activation='gelu')
timeframe_subnet_emb = layers.Dense(HIDDEN_SIZE, activation='gelu')

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
    # net = layers.Dropout(0.2)(net)
    # net = layers.Dense(HIDDEN_SIZE, activation='gelu')(net)
    net = tf.reshape(net, [batch_size, 60, 2])
    net = timeframe_subnet_emb(net)
    slices = create_slices_list(net)
    net = hnode_slice(layers.Flatten()(slices[0]))
    ll = []
    for slice in slices[1:]:
        net1 = net
        slice = hnode_slice(layers.Flatten()(slice))
        net = tf.concat([net, slice], 1)
        net = hnode_subtree(net)
        net = tf.concat([net, net1], 1)
        net = timeframe_subnet_dense(net)
        ll.append(net)
    return tf.concat(ll, 1)

hour = keras.Input(shape=(), dtype=tf.int64, name='hour')
inputs['hour'] = hour
hour = layers.Dense(4, activation='gelu')(tf.one_hot(hour, 24))
day_of_week = keras.Input(shape=(), dtype=tf.int64, name='day_of_week')
inputs['day_of_week'] = day_of_week
day_of_week = layers.Dense(4, activation='gelu')(tf.one_hot(day_of_week, 7))

def hnode(net):
    net1 = layers.Dense(HIDDEN_SIZE, activation='gelu')(net)
    net2 = layers.Dense(HIDDEN_SIZE, activation='sigmoid')(net)
    net3 = layers.Dense(HIDDEN_SIZE, activation='tanh')(net)
    net4 = layers.Dense(HIDDEN_SIZE, activation='swish')(net)
    net5 = layers.Dense(HIDDEN_SIZE, activation='relu')(net)
    net = tf.concat([net1, net2, net3, net4, net5], 1, name='concat_hnode_' + str(i))
    net = layers.Dense(HIDDEN_SIZE, activation='gelu')(net)
    return net

def create_last_timepoint_subnet():
    ll = []
    for f in data.range_features:
        input = keras.Input(shape=(6,), dtype=tf.float32, name=f)
        inputs[f] = input
        ll.append(input)
    net = layers.concatenate(ll, 1)
    for i in range(4):
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
