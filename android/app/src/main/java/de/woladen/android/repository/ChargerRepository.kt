package de.woladen.android.repository

import de.woladen.android.model.AmenityExample
import de.woladen.android.model.ChargerProperties
import de.woladen.android.model.GeoJsonFeature
import de.woladen.android.model.GeoJsonFeatureCollection
import de.woladen.android.model.GeoJsonPointGeometry
import de.woladen.android.model.OperatorCatalog
import de.woladen.android.model.OperatorEntry
import org.json.JSONArray
import org.json.JSONObject

class ChargerRepository(private val dataBundleManager: DataBundleManager) {
    data class LoadResult(
        val features: List<GeoJsonFeature>,
        val operators: List<OperatorEntry>
    )

    suspend fun loadData(): LoadResult {
        val chargersRaw = dataBundleManager.readBundleFile("chargers_fast.geojson")
        val operatorsRaw = dataBundleManager.readBundleFile("operators.json")

        val featureCollection = parseFeatureCollection(chargersRaw)
        val operatorCatalog = parseOperatorCatalog(operatorsRaw)

        val sortedOperators = operatorCatalog.operators.sortedWith(
            compareByDescending<OperatorEntry> { it.stations }.thenBy { it.name }
        )

        return LoadResult(
            features = featureCollection.features,
            operators = sortedOperators
        )
    }

    private fun parseFeatureCollection(raw: String): GeoJsonFeatureCollection {
        val root = JSONObject(raw)
        val generatedAt = root.optString("generated_at", null)
        val featuresArray = root.optJSONArray("features") ?: JSONArray()
        val features = ArrayList<GeoJsonFeature>(featuresArray.length())

        for (i in 0 until featuresArray.length()) {
            val featureObject = featuresArray.optJSONObject(i) ?: continue
            val geometryObject = featureObject.optJSONObject("geometry") ?: continue
            val propertiesObject = featureObject.optJSONObject("properties") ?: continue

            val geometry = GeoJsonPointGeometry(
                type = geometryObject.optString("type", "Point"),
                coordinates = parseCoordinates(geometryObject.optJSONArray("coordinates"))
            )

            val properties = parseProperties(propertiesObject)
            features += GeoJsonFeature(
                id = properties.stationId,
                geometry = geometry,
                properties = properties
            )
        }

        return GeoJsonFeatureCollection(
            generatedAt = generatedAt,
            features = features
        )
    }

    private fun parseCoordinates(array: JSONArray?): List<Double> {
        if (array == null || array.length() < 2) {
            return listOf(0.0, 0.0)
        }
        val lon = parseLossyDouble(array.opt(0), 0.0)
        val lat = parseLossyDouble(array.opt(1), 0.0)
        return listOf(lon, lat)
    }

    private fun parseProperties(json: JSONObject): ChargerProperties {
        val amenityExamplesArray = json.optJSONArray("amenity_examples") ?: JSONArray()
        val amenityExamples = ArrayList<AmenityExample>(amenityExamplesArray.length())
        for (j in 0 until amenityExamplesArray.length()) {
            val item = amenityExamplesArray.optJSONObject(j) ?: continue
            amenityExamples += AmenityExample(
                category = item.optString("category", ""),
                name = item.optString("name").ifBlank { null },
                openingHours = item.optString("opening_hours").ifBlank { null },
                distanceM = parseLossyDoubleOrNull(item.opt("distance_m")),
                lat = parseLossyDoubleOrNull(item.opt("lat")),
                lon = parseLossyDoubleOrNull(item.opt("lon"))
            )
        }

        val amenityCounts = HashMap<String, Int>()
        val keys = json.keys()
        while (keys.hasNext()) {
            val key = keys.next()
            if (!key.startsWith("amenity_")) continue
            val value = parseLossyInt(json.opt(key), 0)
            amenityCounts[key] = value
        }

        val maxPowerKw = parseLossyDouble(json.opt("max_power_kw"), 0.0)

        return ChargerProperties(
            stationId = json.optString("station_id", ""),
            operatorName = json.optString("operator", ""),
            status = json.optString("status", ""),
            maxPowerKw = maxPowerKw,
            chargingPointsCount = parseLossyInt(json.opt("charging_points_count"), 1),
            maxIndividualPowerKw = parseLossyDouble(json.opt("max_individual_power_kw"), maxPowerKw),
            postcode = json.optString("postcode", ""),
            city = json.optString("city", ""),
            address = json.optString("address", ""),
            amenitiesTotal = parseLossyInt(json.opt("amenities_total"), 0),
            amenitiesSource = json.optString("amenities_source", ""),
            amenityExamples = amenityExamples,
            amenityCounts = amenityCounts
        )
    }

    private fun parseOperatorCatalog(raw: String): OperatorCatalog {
        val root = JSONObject(raw)
        val operatorsArray = root.optJSONArray("operators") ?: JSONArray()
        val operators = ArrayList<OperatorEntry>(operatorsArray.length())

        for (i in 0 until operatorsArray.length()) {
            val obj = operatorsArray.optJSONObject(i) ?: continue
            operators += OperatorEntry(
                name = obj.optString("name", ""),
                stations = parseLossyInt(obj.opt("stations"), 0)
            )
        }

        return OperatorCatalog(
            generatedAt = root.optString("generated_at", null),
            minStations = parseLossyInt(root.opt("min_stations"), 0),
            totalOperators = parseLossyInt(root.opt("total_operators"), operators.size),
            operators = operators
        )
    }

    private fun parseLossyInt(any: Any?, fallback: Int): Int {
        return when (any) {
            is Number -> any.toInt()
            is String -> any.replace(',', '.').toDoubleOrNull()?.toInt() ?: fallback
            else -> fallback
        }
    }

    private fun parseLossyDouble(any: Any?, fallback: Double): Double {
        return when (any) {
            is Number -> any.toDouble()
            is String -> any.replace(',', '.').toDoubleOrNull() ?: fallback
            else -> fallback
        }
    }

    private fun parseLossyDoubleOrNull(any: Any?): Double? {
        return when (any) {
            null -> null
            JSONObject.NULL -> null
            is Number -> any.toDouble()
            is String -> any.replace(',', '.').toDoubleOrNull()
            else -> null
        }
    }
}
