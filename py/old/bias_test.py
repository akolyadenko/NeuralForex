import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
import random

batch_size = 1024
def random_generator():
    for i in range(100000):
        yield (
            # [random.randint(0,10) for i in range(60)],
            [random.randint(750,770) for i in range(60)],
            [1]*20+[0]*80)

def create_ds():
    ds = tf.data.Dataset.from_generator(random_generator,
                                        (tf.float32, tf.float32),
                                        ((60), (100)))
    ds = ds.batch(batch_size, drop_remainder=True)
    ds = ds.cache()
    return ds

inputs = keras.Input(shape=(60,), dtype=tf.float32, batch_size=batch_size)
net = layers.Dense(100, activation='relu')(inputs)
net = layers.Dense(100, activation='relu')(net)
model = keras.Model(inputs=inputs, outputs=net)

model.summary()

model.compile(loss=tf.keras.losses.MeanSquaredError(), optimizer=keras.optimizers.SGD(learning_rate=1e-3))
model.fit(create_ds(), epochs=3000)

for (x, y) in create_ds():
    predictions = model.predict(x)
    for p in predictions:
        print('predictions: ' + str(p))
    break