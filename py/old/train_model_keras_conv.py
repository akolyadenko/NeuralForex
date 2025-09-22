import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
from strategies.prod.eurusd_buy import data

batch_size = 1024

# for t in data.timeframes:
#    tl = []
#    for f in data.indicators:
#        feature = t + f + '_rescaled'
#        input = keras.Input(shape=(60,), dtype=tf.float32, name=feature)
#        inputs[feature] = input
#        input = layers.Reshape((60,1))(input)
#        tl.append(input)
#    net = layers.concatenate(tl, 2)
#    net = layers.Conv1D(16, 5, activation='sigmoid')(net)
#    net = layers.Conv1D(16, 5)(net)
#    net = layers.Conv1D(16, 5)(net)
#    net = layers.Flatten()(net)
#    l.append(net)

class TransformerEncoder(layers.Layer):
    def __init__(self, embed_dim, num_heads, feed_forward_dim, rate=0.1):
        super().__init__()
        self.att = layers.MultiHeadAttention(num_heads=num_heads, key_dim=embed_dim)
        self.ffn = keras.Sequential(
            [
                layers.Dense(feed_forward_dim, activation="relu"),
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

inputs = {}
l = []

for f in data.timeframe_features:
    input = keras.Input(shape=(60,), dtype=tf.float32, name=f)
    inputs[f] = input
    # l.append(input)
    net = layers.Reshape((60,1))(input)
    for i in range(8):
        net = layers.Conv1D(32, 7)(net)
        l.append(layers.Flatten()(net))

net = layers.concatenate(l, 1)
# net = layers.Dense(400, activation='swish')(net)
for i in range(2):
    # net = TransformerEncoder(4, 4, 4)(net)
    net = layers.Dense(512, activation='swish')(net)
    net = layers.Dropout(0.1)(net)
net = layers.Flatten()(net)
net = layers.Dense(100, activation='relu')(net)
model = keras.Model(inputs=inputs, outputs=net)

model.summary()

model.compile(loss=tf.keras.losses.MeanSquaredError(), optimizer=keras.optimizers.Adam(learning_rate=1e-4))

# model = keras.models.load_model('/usr/proj/trd/model')

model.fit(
    data.create_ds('train', batch_size),
    epochs=55,
    validation_data=data.create_ds('test', batch_size))

for (x, y) in data.create_ds('test', batch_size):
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

model.save('/usr/proj/trd/model')