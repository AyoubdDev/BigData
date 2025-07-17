import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.common.utils.Bytes;

import java.io.IOException;
import java.util.Properties;

public class WeatherStreamApp {

    // ========================================================================
    // Classe de données interne pour représenter les données météo
    // ========================================================================
    public static class WeatherData {
        private String station;
        private double temperature;
        private double humidity;

        // Constructeur vide requis pour la désérialisation
        public WeatherData() {}

        public WeatherData(String station, double temperature, double humidity) {
            this.station = station;
            this.temperature = temperature;
            this.humidity = humidity;
        }

        public String getStation() { return station; }
        public void setStation(String station) { this.station = station; }
        public double getTemperature() { return temperature; }
        public void setTemperature(double temperature) { this.temperature = temperature; }
        public double getHumidity() { return humidity; }
        public void setHumidity(double humidity) { this.humidity = humidity; }

        @Override
        public String toString() {
            return "WeatherData{station='" + station + "', temperature=" + temperature + ", humidity=" + humidity + '}';
        }
    }

    // ========================================================================
    // Classe de données interne pour l'agrégation
    // ========================================================================
    public static class WeatherStats {
        private double tempSum = 0.0;
        private double humiditySum = 0.0;
        private int count = 0;

        public double getTempSum() { return tempSum; }
        public void setTempSum(double tempSum) { this.tempSum = tempSum; }
        public double getHumiditySum() { return humiditySum; }
        public void setHumiditySum(double humiditySum) { this.humiditySum = humiditySum; }
        public int getCount() { return count; }
        public void setCount(int count) { this.count = count; }

        public WeatherStats update(WeatherData data) {
            this.tempSum += data.getTemperature();
            this.humiditySum += data.getHumidity();
            this.count++;
            return this;
        }
    }

    // ========================================================================
    // SerDe JSON interne pour sérialiser/désérialiser les objets ci-dessus
    // ========================================================================
    public static class JsonSerde<T> implements Serde<T> {
        private final ObjectMapper objectMapper = new ObjectMapper();
        private final Class<T> tClass;

        public JsonSerde(Class<T> tClass) {
            this.tClass = tClass;
        }

        @Override
        public Serializer<T> serializer() {
            return (topic, data) -> {
                try {
                    return objectMapper.writeValueAsBytes(data);
                } catch (Exception e) {
                    throw new SerializationException("Erreur lors de la sérialisation en JSON", e);
                }
            };
        }

        @Override
        public Deserializer<T> deserializer() {
            return (topic, data) -> {
                if (data == null) return null;
                try {
                    return objectMapper.readValue(data, tClass);
                } catch (IOException e) {
                    throw new SerializationException("Erreur lors de la désérialisation du JSON", e);
                }
            };
        }
    }


    // ========================================================================
    // Méthode principale de l'application Kafka Streams
    // ========================================================================
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "weather-stream-app-single-file");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder builder = new StreamsBuilder();

        // Utilisation de nos classes internes pour les SerDes
        final Serde<WeatherData> weatherDataSerde = new JsonSerde<>(WeatherData.class);
        final Serde<WeatherStats> weatherStatsSerde = new JsonSerde<>(WeatherStats.class);

        // Topic d'entrée contenant des messages au format "station,temperature,humidity"
        KStream<String, String> rawWeatherStream = builder.stream("weather-data");

        // Étape 1: Parser la chaîne brute en objet WeatherData et définir la clé
        KStream<String, WeatherData> weatherStream = rawWeatherStream.map((key, value) -> {
            try {
                String[] parts = value.split(",");
                WeatherData data = new WeatherData(parts[0], Double.parseDouble(parts[1]), Double.parseDouble(parts[2]));
                return KeyValue.pair(data.getStation(), data);
            } catch (Exception e) {
                System.err.println("Message mal formé, ignoré: " + value);
                return KeyValue.pair(null, null);
            }
        }).filter((key, value) -> key != null);

        // Étape 2: Filtrer les températures > 30°C
        KStream<String, WeatherData> filtered = weatherStream.filter((key, data) -> data.getTemperature() > 30);

        // Étape 3: Convertir la température de Celsius en Fahrenheit
        KStream<String, WeatherData> converted = filtered.mapValues(data -> {
            double fahrenheit = (data.getTemperature() * 9 / 5) + 32;
            data.setTemperature(fahrenheit);
            return data;
        });

        converted.peek((k, v) -> System.out.println("🌡️ Converti en °F: " + v));

        // Étape 4: Grouper par clé de station
        KGroupedStream<String, WeatherData> grouped = converted.groupByKey(Grouped.with(Serdes.String(), weatherDataSerde));

        // Étape 5: Agréger les données
        KTable<String, WeatherStats> aggregated = grouped.aggregate(
                WeatherStats::new,
                (key, newData, stats) -> stats.update(newData),
                Materialized.<String, WeatherStats, KeyValueStore<Bytes, byte[]>>as("weather-aggregation-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(weatherStatsSerde)
        );

        // Étape 6: Calculer les moyennes et formater la sortie
        KTable<String, String> result = aggregated.mapValues(stats -> {
            if (stats.getCount() == 0) return null;
            double avgTemp = stats.getTempSum() / stats.getCount();
            double avgHumidity = stats.getHumiditySum() / stats.getCount();
            return String.format("Température Moyenne = %.2f°F, Humidité Moyenne = %.2f%%", avgTemp, avgHumidity);
        });

        result.toStream().peek((k, v) -> System.out.println("📊 Résultat final pour " + k + ": " + v));

        // Étape 7: Envoyer le résultat vers le topic de sortie
        result.toStream().to("station-averages", Produced.with(Serdes.String(), Serdes.String()));

        // Démarrer l'application
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // Arrêter proprement l'application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}