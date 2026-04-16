package de.woladen.android.service

import android.util.JsonReader
import android.util.JsonToken
import de.woladen.android.model.AvailabilityStatus
import de.woladen.android.model.LiveEvse
import de.woladen.android.model.LiveJsonValue
import de.woladen.android.model.LiveStationDetail
import de.woladen.android.model.LiveStationLookupResponse
import de.woladen.android.model.LiveStationSummary
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.io.IOException
import java.io.Reader
import java.net.HttpURLConnection
import java.net.URL
import java.net.URLEncoder

class LiveApiClient(
    private val baseUrl: String = DEFAULT_BASE_URL
) {
    val isEnabled: Boolean
        get() = baseUrl.isNotBlank()

    suspend fun lookupStations(stationIds: List<String>): LiveStationLookupResponse = withContext(Dispatchers.IO) {
        val connection = openConnection(
            path = "/v1/stations/lookup",
            method = "POST",
            timeoutMs = LOOKUP_TIMEOUT_MS
        )
        connection.setRequestProperty("Accept", "application/json")
        connection.setRequestProperty("Content-Type", "application/json")
        connection.doOutput = true
        connection.outputStream.bufferedWriter().use { writer ->
            writer.write(lookupRequestBody(stationIds))
        }
        readResponse(connection, ::parseLookupResponse)
    }

    suspend fun stationDetail(stationId: String): LiveStationDetail = withContext(Dispatchers.IO) {
        val encodedStationId = URLEncoder.encode(stationId, Charsets.UTF_8.name()).replace("+", "%20")
        val connection = openConnection(
            path = "/v1/stations/$encodedStationId",
            method = "GET",
            timeoutMs = DETAIL_TIMEOUT_MS
        )
        connection.setRequestProperty("Accept", "application/json")
        readResponse(connection, ::parseStationDetail)
    }

    private fun openConnection(path: String, method: String, timeoutMs: Int): HttpURLConnection {
        val normalizedBaseUrl = baseUrl.trimEnd('/')
        val connection = URL("$normalizedBaseUrl$path").openConnection() as HttpURLConnection
        connection.requestMethod = method
        connection.connectTimeout = timeoutMs
        connection.readTimeout = timeoutMs
        connection.instanceFollowRedirects = true
        return connection
    }

    private fun <T> readResponse(connection: HttpURLConnection, parser: (Reader) -> T): T {
        try {
            val statusCode = connection.responseCode
            if (statusCode !in 200..299) {
                throw IOException("HTTP $statusCode")
            }
            connection.inputStream.bufferedReader().use { reader ->
                return parser(reader)
            }
        } finally {
            connection.disconnect()
        }
    }

    private fun lookupRequestBody(stationIds: List<String>): String {
        val ids = stationIds.joinToString(",") { id -> "\"${escapeJson(id)}\"" }
        return "{\"station_ids\":[$ids]}"
    }

    private fun parseLookupResponse(reader: Reader): LiveStationLookupResponse {
        JsonReader(reader).use { jsonReader ->
            val stations = mutableListOf<LiveStationSummary>()
            val missingStationIds = mutableListOf<String>()

            jsonReader.beginObject()
            while (jsonReader.hasNext()) {
                when (jsonReader.nextName()) {
                    "stations" -> {
                        jsonReader.beginArray()
                        while (jsonReader.hasNext()) {
                            stations += parseLiveStationSummary(jsonReader)
                        }
                        jsonReader.endArray()
                    }
                    "missing_station_ids" -> {
                        jsonReader.beginArray()
                        while (jsonReader.hasNext()) {
                            nextStringOrNull(jsonReader)?.takeIf { it.isNotBlank() }?.let(missingStationIds::add)
                        }
                        jsonReader.endArray()
                    }
                    else -> jsonReader.skipValue()
                }
            }
            jsonReader.endObject()

            return LiveStationLookupResponse(
                stations = stations,
                missingStationIds = missingStationIds
            )
        }
    }

    private fun parseStationDetail(reader: Reader): LiveStationDetail {
        JsonReader(reader).use { jsonReader ->
            var station: LiveStationSummary? = null
            val evses = mutableListOf<LiveEvse>()

            jsonReader.beginObject()
            while (jsonReader.hasNext()) {
                when (jsonReader.nextName()) {
                    "station" -> station = parseLiveStationSummary(jsonReader)
                    "evses" -> {
                        jsonReader.beginArray()
                        while (jsonReader.hasNext()) {
                            evses += parseLiveEvse(jsonReader)
                        }
                        jsonReader.endArray()
                    }
                    else -> jsonReader.skipValue()
                }
            }
            jsonReader.endObject()

            return LiveStationDetail(
                station = station ?: throw IOException("missing station in live detail"),
                evses = evses
            )
        }
    }

    private fun parseLiveStationSummary(reader: JsonReader): LiveStationSummary {
        var stationId = ""
        var availabilityStatus = AvailabilityStatus.UNKNOWN
        var availableEvses = 0
        var occupiedEvses = 0
        var outOfOrderEvses = 0
        var unknownEvses = 0
        var totalEvses = 0
        var priceDisplay = ""
        var priceCurrency = ""
        var priceEnergyEurKwhMin = ""
        var priceEnergyEurKwhMax = ""
        var sourceObservedAt = ""
        var fetchedAt = ""
        var ingestedAt = ""

        reader.beginObject()
        while (reader.hasNext()) {
            when (reader.nextName()) {
                "station_id" -> stationId = nextStringOrNull(reader).orEmpty()
                "availability_status" -> availabilityStatus = AvailabilityStatus.fromRaw(nextStringOrNull(reader))
                "available_evses" -> availableEvses = nextLossyInt(reader, 0)
                "occupied_evses" -> occupiedEvses = nextLossyInt(reader, 0)
                "out_of_order_evses" -> outOfOrderEvses = nextLossyInt(reader, 0)
                "unknown_evses" -> unknownEvses = nextLossyInt(reader, 0)
                "total_evses" -> totalEvses = nextLossyInt(reader, 0)
                "price_display" -> priceDisplay = nextStringOrNull(reader).orEmpty()
                "price_currency" -> priceCurrency = nextStringOrNull(reader).orEmpty()
                "price_energy_eur_kwh_min" -> priceEnergyEurKwhMin = nextStringOrNull(reader).orEmpty()
                "price_energy_eur_kwh_max" -> priceEnergyEurKwhMax = nextStringOrNull(reader).orEmpty()
                "source_observed_at" -> sourceObservedAt = nextStringOrNull(reader).orEmpty()
                "fetched_at" -> fetchedAt = nextStringOrNull(reader).orEmpty()
                "ingested_at" -> ingestedAt = nextStringOrNull(reader).orEmpty()
                else -> reader.skipValue()
            }
        }
        reader.endObject()

        return LiveStationSummary(
            stationId = stationId,
            availabilityStatus = availabilityStatus,
            availableEvses = availableEvses,
            occupiedEvses = occupiedEvses,
            outOfOrderEvses = outOfOrderEvses,
            unknownEvses = unknownEvses,
            totalEvses = totalEvses,
            priceDisplay = priceDisplay,
            priceCurrency = priceCurrency,
            priceEnergyEurKwhMin = priceEnergyEurKwhMin,
            priceEnergyEurKwhMax = priceEnergyEurKwhMax,
            sourceObservedAt = sourceObservedAt,
            fetchedAt = fetchedAt,
            ingestedAt = ingestedAt
        )
    }

    private fun parseLiveEvse(reader: JsonReader): LiveEvse {
        var providerEvseId = ""
        var availabilityStatus = AvailabilityStatus.UNKNOWN
        var operationalStatus = ""
        var priceDisplay = ""
        var sourceObservedAt = ""
        var fetchedAt = ""
        var ingestedAt = ""
        var nextAvailableChargingSlots: List<LiveJsonValue> = emptyList()
        var supplementalFacilityStatus: List<LiveJsonValue> = emptyList()

        reader.beginObject()
        while (reader.hasNext()) {
            when (reader.nextName()) {
                "provider_evse_id" -> providerEvseId = nextStringOrNull(reader).orEmpty()
                "availability_status" -> availabilityStatus = AvailabilityStatus.fromRaw(nextStringOrNull(reader))
                "operational_status" -> operationalStatus = nextStringOrNull(reader).orEmpty()
                "price_display" -> priceDisplay = nextStringOrNull(reader).orEmpty()
                "source_observed_at" -> sourceObservedAt = nextStringOrNull(reader).orEmpty()
                "fetched_at" -> fetchedAt = nextStringOrNull(reader).orEmpty()
                "ingested_at" -> ingestedAt = nextStringOrNull(reader).orEmpty()
                "next_available_charging_slots" -> nextAvailableChargingSlots = parseLiveJsonArray(reader)
                "supplemental_facility_status" -> supplementalFacilityStatus = parseLiveJsonArray(reader)
                else -> reader.skipValue()
            }
        }
        reader.endObject()

        return LiveEvse(
            providerEvseId = providerEvseId,
            availabilityStatus = availabilityStatus,
            operationalStatus = operationalStatus,
            priceDisplay = priceDisplay,
            sourceObservedAt = sourceObservedAt,
            fetchedAt = fetchedAt,
            ingestedAt = ingestedAt,
            nextAvailableChargingSlots = nextAvailableChargingSlots,
            supplementalFacilityStatus = supplementalFacilityStatus
        )
    }

    private fun parseLiveJsonArray(reader: JsonReader): List<LiveJsonValue> {
        if (reader.peek() == JsonToken.NULL) {
            reader.nextNull()
            return emptyList()
        }

        val items = mutableListOf<LiveJsonValue>()
        reader.beginArray()
        while (reader.hasNext()) {
            items += parseLiveJsonValue(reader)
        }
        reader.endArray()
        return items
    }

    private fun parseLiveJsonValue(reader: JsonReader): LiveJsonValue {
        return when (reader.peek()) {
            JsonToken.BEGIN_OBJECT -> {
                val entries = LinkedHashMap<String, LiveJsonValue>()
                reader.beginObject()
                while (reader.hasNext()) {
                    entries[reader.nextName()] = parseLiveJsonValue(reader)
                }
                reader.endObject()
                LiveJsonValue.ObjectValue(entries)
            }
            JsonToken.BEGIN_ARRAY -> {
                val items = mutableListOf<LiveJsonValue>()
                reader.beginArray()
                while (reader.hasNext()) {
                    items += parseLiveJsonValue(reader)
                }
                reader.endArray()
                LiveJsonValue.ArrayValue(items)
            }
            JsonToken.BOOLEAN -> LiveJsonValue.BoolValue(reader.nextBoolean())
            JsonToken.NUMBER -> LiveJsonValue.NumberValue(nextLossyDouble(reader, 0.0))
            JsonToken.STRING -> LiveJsonValue.StringValue(reader.nextString())
            JsonToken.NULL -> {
                reader.nextNull()
                LiveJsonValue.NullValue
            }
            else -> {
                reader.skipValue()
                LiveJsonValue.NullValue
            }
        }
    }

    private fun nextStringOrNull(reader: JsonReader): String? {
        return when (reader.peek()) {
            JsonToken.STRING -> reader.nextString()
            JsonToken.NUMBER -> reader.nextString()
            JsonToken.BOOLEAN -> reader.nextBoolean().toString()
            JsonToken.NULL -> {
                reader.nextNull()
                null
            }
            else -> {
                reader.skipValue()
                null
            }
        }
    }

    private fun nextLossyInt(reader: JsonReader, fallback: Int): Int {
        return when (reader.peek()) {
            JsonToken.NUMBER -> reader.nextDouble().toInt()
            JsonToken.STRING -> {
                val value = reader.nextString()
                value.toIntOrNull() ?: value.toDoubleOrNull()?.toInt() ?: fallback
            }
            JsonToken.NULL -> {
                reader.nextNull()
                fallback
            }
            else -> {
                reader.skipValue()
                fallback
            }
        }
    }

    private fun nextLossyDouble(reader: JsonReader, fallback: Double): Double {
        return when (reader.peek()) {
            JsonToken.NUMBER -> reader.nextDouble()
            JsonToken.STRING -> reader.nextString().replace(',', '.').toDoubleOrNull() ?: fallback
            JsonToken.NULL -> {
                reader.nextNull()
                fallback
            }
            else -> {
                reader.skipValue()
                fallback
            }
        }
    }

    private fun escapeJson(value: String): String {
        return buildString {
            value.forEach { character ->
                when (character) {
                    '\\' -> append("\\\\")
                    '"' -> append("\\\"")
                    '\n' -> append("\\n")
                    '\r' -> append("\\r")
                    '\t' -> append("\\t")
                    else -> append(character)
                }
            }
        }
    }

    companion object {
        private const val DEFAULT_BASE_URL = "https://live.woladen.de"
        private const val LOOKUP_TIMEOUT_MS = 3_500
        private const val DETAIL_TIMEOUT_MS = 4_000
    }
}
