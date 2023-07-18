Adapter
=======

.. autofunction:: hivecore.patterns.adapter

Adaptee
^^^^^^^

.. autofunction:: hivecore.patterns.adaptee

Example
^^^^^^^

We create an adaptee class `CelsiusTemperatureSensor`. For this example, we want to adapt this celsius sensor to both fahrenheit and kelvin.

.. code-block:: python

    from hivecore.patterns import adaptee

    @adaptee
    class CelsiusTemperatureSensor:
        def get_temperature(self):
            return 25  # Celsius

Creating the Adapter
~~~~~~~~~~~~~~~~~~~~

We define `FahrenheitTemperatureSensor` and `KelvinTemperatureSensor` as adapters for the `CelsiusTemperatureSensor`.

.. code-block:: python

    from hivecore.patterns import adapter

    def celsius_to_fahrenheit(celsius):
        return 9.0 / 5.0 * celsius + 32

    @adapter("CelsiusTemperatureSensor", get_temperature="get_temperature")
    class FahrenheitTemperatureSensor:
        def get_temperature(self):
            celsius = self._adaptee.get_temperature()
            return celsius_to_fahrenheit(celsius)

    @adapter("CelsiusTemperatureSensor", get_temperature="get_temperature")
    class KelvinTemperatureSensor:
        def get_temperature(self):
            celsius = self._adaptee.get_temperature()
            return celsius + 273.15

Using the Adapter
~~~~~~~~~~~~~~~~~

.. code-block:: python

    # Create adapter objects
    fahrenheit_sensor = FahrenheitTemperatureSensor()
    kelvin_sensor = KelvinTemperatureSensor()

    # Get temperature in different units
    print(fahrenheit_sensor.get_temperature())
    print(kelvin_sensor.get_temperature())

Output
------

.. code-block:: text

    77.0
    298.15