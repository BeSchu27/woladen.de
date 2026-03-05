package de.woladen.android.util

data class AmenityMeta(
    val key: String,
    val label: String
)

object AmenityCatalog {
    val all: List<AmenityMeta> = listOf(
        AmenityMeta("amenity_restaurant", "Restaurant"),
        AmenityMeta("amenity_cafe", "Café"),
        AmenityMeta("amenity_fast_food", "Fast Food"),
        AmenityMeta("amenity_toilets", "Toiletten"),
        AmenityMeta("amenity_supermarket", "Supermarkt"),
        AmenityMeta("amenity_bakery", "Bäckerei"),
        AmenityMeta("amenity_convenience", "Kiosk"),
        AmenityMeta("amenity_pharmacy", "Apotheke"),
        AmenityMeta("amenity_hotel", "Hotel"),
        AmenityMeta("amenity_museum", "Museum"),
        AmenityMeta("amenity_playground", "Spielplatz"),
        AmenityMeta("amenity_park", "Park"),
        AmenityMeta("amenity_ice_cream", "Eis")
    )

    private val byKey: Map<String, AmenityMeta> = all.associateBy { it.key }

    fun labelFor(key: String): String {
        return byKey[key]?.label ?: key.removePrefix("amenity_")
    }
}
