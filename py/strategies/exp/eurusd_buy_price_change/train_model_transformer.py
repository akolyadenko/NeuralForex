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
HIDDEN_SIZE = 64
model_path = '/usr/proj/trd/models/exp/base/'
render_predictions_lib.create_schema()

shared_hnode_dense_slice_1 = layers.Dense(HIDDEN_SIZE, activation='gelu')
shared_hnode_dense_slice_2 = layers.Dense(HIDDEN_SIZE, activation='gelu')

shared_hnode_slice_layers = []

for i in range(4):
    layer = {}
    layer['relu'] = layers.Dense(HIDDEN_SIZE * 4, activation='relu')
    layer['gelu'] = layers.Dense(HIDDEN_SIZE * 4, activation='gelu')
    layer['sigmoid'] = layers.Dense(HIDDEN_SIZE * 4, activation='sigmoid')
    layer['tanh'] = layers.Dense(HIDDEN_SIZE * 4, activation='tanh')
    layer['swish'] = layers.Dense(HIDDEN_SIZE * 4, activation='swish')
    layer['dense'] = layers.Dense(HIDDEN_SIZE, activation='relu')
    shared_hnode_slice_layers.append(layer)

def hnode_slice(net):
    # net = layers.Dropout(0.5)(net)
    net = shared_hnode_dense_slice_1(net)
    net = shared_hnode_dense_slice_2(net)

    for layer in shared_hnode_slice_layers:
        net1 = layer['relu'](net)
        net2 = layer['sigmoid'](net)
        net3 = layer['tanh'](net)
        net4 = layer['swish'](net)
        net5 = layer['gelu'](net)
        net = tf.concat([net1, net2, net3, net4, net5], 1, name='concat_hnode')
        # net = layers.Dense(HIDDEN_SIZE, activation='relu')(net)
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

class TransformerEncoder(layers.Layer):
    def __init__(self, embed_dim, num_heads, feed_forward_dim, rate=0.1):
        super().__init__()
        self.att = layers.MultiHeadAttention(num_heads=num_heads, key_dim=embed_dim)
        self.ffn = keras.Sequential(
            [
                layers.Dense(feed_forward_dim, activation="gelu"),
                layers.Dense(embed_dim),
            ]
        )
        self.layernorm1 = layers.LayerNormalization(epsilon=1e-6)
        self.layernorm2 = layers.LayerNormalization(epsilon=1e-6)
        self.dropout1 = layers.Dropout(rate)
        self.dropout2 = layers.Dropout(rate)

    def call(self, inputs):
        attn_output = self.att(inputs, inputs)
        attn_output = self.dropout1(attn_output)
        out1 = self.layernorm1(inputs + attn_output)
        ffn_output = self.ffn(out1)
        ffn_output = self.dropout2(ffn_output)
        return self.layernorm2(out1 + ffn_output)

timeframe_subnet_dense = layers.Dense(HIDDEN_SIZE, activation='gelu')

shared_hnode_dense_emb_1 = layers.Dense(HIDDEN_SIZE, activation='gelu')
shared_hnode_dense_emb_2 = layers.Dense(HIDDEN_SIZE, activation='gelu')

def create_timeframe_subnet(t):
    ll = []
    # vfor i in ['open', 'close', 'high', 'low']:
    for i in ['high', 'low']:
        f = t + i + '_rescaled'
        input = keras.Input(shape=(60,), dtype=tf.float32, name=f)
        inputs[f] = input
        input = layers.Reshape((60,1))(input)
        ll.append(input)
    net = layers.concatenate(ll, 2)
    net = shared_hnode_dense_emb_1(net)
    net = shared_hnode_dense_emb_2(net)
    return net + keras_nlp.layers.PositionEmbedding(60)(net)

hour = keras.Input(shape=(), dtype=tf.int64, name='hour')
inputs['hour'] = hour
hour = layers.Dense(HIDDEN_SIZE, activation='relu')(tf.one_hot(hour, 24))
hour = tf.expand_dims(hour, 1)
day_of_week = keras.Input(shape=(), dtype=tf.int64, name='day_of_week')
inputs['day_of_week'] = day_of_week
day_of_week = layers.Dense(HIDDEN_SIZE, activation='relu')(tf.one_hot(day_of_week, 7))
day_of_week = tf.expand_dims(day_of_week, 1)

def create_last_timepoint_subnet():
    ll = []
    for f in data.range_features:
        input = keras.Input(shape=(6,), dtype=tf.float32, name=f)
        inputs[f] = input
        input = layers.Reshape((6, 1))(input)
        ll.append(input)
    net = layers.concatenate(ll, 2)
    for i in range(2):
        net = layers.Dense(HIDDEN_SIZE, activation='gelu')(net)
    return net

net = tf.concat([
    # create_timeframe_subnet('m1'),
    create_timeframe_subnet('m5'),
    create_timeframe_subnet('m15'),
    create_timeframe_subnet('m30'),
    create_last_timepoint_subnet(),
    hour,
    day_of_week
    ], 1)

# net = create_last_timepoint_subnet()

for i in range(8):
    net = TransformerEncoder(HIDDEN_SIZE, 8, HIDDEN_SIZE)(net)

net = layers.Flatten()(net)

#net = tf.concat([
#    net,
#    hour,
#    day_of_week], 1)

# net = layers.concatenate(l, 1,  name='concat3')
# net = layers.Dropout(0.2)(net)
net = layers.Dense(HIDDEN_SIZE, activation='gelu')(net)
net = layers.Flatten()(net)
net = layers.Dense(20, activation='gelu')(net)
net = layers.Dense(20, activation='gelu')(net)
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

compile_and_fit(100, 1e-3, train_ds, test_ds)
# compile_and_fit(5, 1e-5, train_ds, test_ds)

print('min_val_loss = ' + str(min_val_loss))
print('min_val_loss_epoch = ' + str(min_val_loss_epoch))

model.save(model_path + str(saved_checkpoint))

# render_predictions_lib.render_predictions(model_path,batch_size, saved_checkpoint)
