package de.woladen.android.model

data class DataBundleManifest(
    val version: String,
    val generatedAt: String,
    val schema: String
) {
    companion object {
        val BASELINE = DataBundleManifest(
            version = "baseline",
            generatedAt = "unknown",
            schema = "chargers_fast.geojson+operators.json"
        )
    }
}

data class ActiveDataBundleInfo(
    val source: String,
    val manifest: DataBundleManifest
)
