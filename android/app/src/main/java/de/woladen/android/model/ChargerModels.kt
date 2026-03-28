package de.woladen.android.model

private const val MAX_REASONABLE_DISPLAY_POWER_KW = 400.0

data class GeoJsonFeatureCollection(
    val generatedAt: String?,
    val features: List<GeoJsonFeature>
)

data class GeoJsonFeature(
    val id: String,
    val geometry: GeoJsonPointGeometry,
    val properties: ChargerProperties
) {
    val latitude: Double get() = geometry.latitude
    val longitude: Double get() = geometry.longitude
}

data class GeoJsonPointGeometry(
    val type: String,
    val coordinates: List<Double>
) {
    val longitude: Double get() = if (coordinates.size == 2) coordinates[0] else 0.0
    val latitude: Double get() = if (coordinates.size == 2) coordinates[1] else 0.0
}

data class ChargerProperties(
    val stationId: String,
    val operatorName: String,
    val status: String,
    val maxPowerKw: Double,
    val chargingPointsCount: Int,
    val maxIndividualPowerKw: Double,
    val postcode: String,
    val city: String,
    val address: String,
    val amenitiesTotal: Int,
    val amenitiesSource: String,
    val amenityExamples: List<AmenityExample>,
    val amenityCounts: Map<String, Int>
) {
    val displayedMaxPowerKw: Double
        get() {
            val maxIndividual = sanitizeDisplayedPowerKw(maxIndividualPowerKw)
            if (maxIndividual > 0.0) {
                return maxIndividual
            }
            return sanitizeDisplayedPowerKw(maxPowerKw)
        }

    fun topAmenities(limit: Int = 3): List<AmenityCount> {
        return amenityCounts
            .filterValues { it > 0 }
            .map { AmenityCount(it.key, it.value) }
            .sortedWith(compareByDescending<AmenityCount> { it.count }.thenBy { it.key })
            .take(limit)
    }
}

data class AmenityExample(
    val category: String,
    val name: String?,
    val openingHours: String?,
    val distanceM: Double?,
    val lat: Double?,
    val lon: Double?
)

data class AmenityCount(
    val key: String,
    val count: Int
)

private fun sanitizeDisplayedPowerKw(value: Double): Double {
    if (!value.isFinite() || value <= 0.0) {
        return 0.0
    }
    return minOf(value, MAX_REASONABLE_DISPLAY_POWER_KW)
}
