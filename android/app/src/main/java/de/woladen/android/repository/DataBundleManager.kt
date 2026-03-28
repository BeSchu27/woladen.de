package de.woladen.android.repository

import android.content.Context
import android.net.Uri
import androidx.documentfile.provider.DocumentFile
import de.woladen.android.model.ActiveDataBundleInfo
import de.woladen.android.model.DataBundleManifest
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.json.JSONObject
import java.io.File
import java.io.IOException
import java.io.Reader
import java.time.Instant

class DataBundleManager(private val context: Context) {
    private val installedRelativePath = "WoladenDataBundle/current"
    private val requiredBundleFiles = listOf("chargers_fast.geojson", "operators.json")

    suspend fun activeBundleInfo(): ActiveDataBundleInfo = withContext(Dispatchers.IO) {
        if (hasValidInstalledBundle()) {
            val manifest = loadInstalledManifest() ?: DataBundleManifest(
                version = "custom",
                generatedAt = "unknown",
                schema = "chargers_fast.geojson+operators.json"
            )
            ActiveDataBundleInfo(source = "installed", manifest = manifest)
        } else {
            val manifest = loadAssetManifest() ?: DataBundleManifest.BASELINE
            ActiveDataBundleInfo(source = "baseline", manifest = manifest)
        }
    }

    suspend fun readBundleFile(fileName: String): String = withContext(Dispatchers.IO) {
        if (hasValidInstalledBundle()) {
            val file = File(installedBundleDir(), fileName)
            if (file.exists()) {
                return@withContext file.readText()
            }
        }
        context.assets.open(fileName).bufferedReader().use { it.readText() }
    }

    suspend fun <T> useBundleReader(fileName: String, block: (Reader) -> T): T = withContext(Dispatchers.IO) {
        if (hasValidInstalledBundle()) {
            val file = File(installedBundleDir(), fileName)
            if (file.exists()) {
                return@withContext file.bufferedReader().use(block)
            }
        }
        context.assets.open(fileName).bufferedReader().use(block)
    }

    suspend fun installBundleFromTreeUri(treeUri: Uri): Unit = withContext(Dispatchers.IO) {
        val tree = DocumentFile.fromTreeUri(context, treeUri)
            ?: throw IOException("Ungültiger Ordner")

        val chargerFile = tree.findFile("chargers_fast.geojson")
            ?: throw IOException("chargers_fast.geojson fehlt")
        val operatorsFile = tree.findFile("operators.json")
            ?: throw IOException("operators.json fehlt")
        val manifestFile = tree.findFile("data_manifest.json")

        val destination = installedBundleDir()
        if (destination.exists()) {
            destination.deleteRecursively()
        }
        if (!destination.mkdirs()) {
            throw IOException("Bundle-Zielordner konnte nicht erstellt werden")
        }

        copyDocumentToFile(chargerFile, File(destination, "chargers_fast.geojson"))
        copyDocumentToFile(operatorsFile, File(destination, "operators.json"))

        val manifestTarget = File(destination, "data_manifest.json")
        if (manifestFile != null) {
            copyDocumentToFile(manifestFile, manifestTarget)
        } else {
            val fallbackManifest = DataBundleManifest(
                version = "local-import-${Instant.now()}",
                generatedAt = Instant.now().toString(),
                schema = "chargers_fast.geojson+operators.json"
            )
            manifestTarget.writeText(
                JSONObject()
                    .put("version", fallbackManifest.version)
                    .put("generatedAt", fallbackManifest.generatedAt)
                    .put("schema", fallbackManifest.schema)
                    .toString()
            )
        }

        loadInstalledManifest()
            ?: throw IOException("data_manifest.json ist ungültig")
    }

    suspend fun removeInstalledBundle(): Unit = withContext(Dispatchers.IO) {
        val destination = installedBundleDir()
        if (destination.exists()) {
            destination.deleteRecursively()
        }
    }

    private fun hasValidInstalledBundle(): Boolean {
        val folder = installedBundleDir()
        return folder.exists() && requiredBundleFiles.all { fileName ->
            File(folder, fileName).exists()
        }
    }

    private fun installedBundleDir(): File {
        return File(context.filesDir, installedRelativePath)
    }

    private fun loadInstalledManifest(): DataBundleManifest? {
        val manifestFile = File(installedBundleDir(), "data_manifest.json")
        if (!manifestFile.exists()) return null
        return parseManifest(manifestFile.readText())
    }

    private fun loadAssetManifest(): DataBundleManifest? {
        return try {
            val raw = context.assets.open("data_manifest.json").bufferedReader().use { it.readText() }
            parseManifest(raw)
        } catch (_: Exception) {
            null
        }
    }

    private fun parseManifest(raw: String): DataBundleManifest? {
        return try {
            val json = JSONObject(raw)
            DataBundleManifest(
                version = json.optString("version", "unknown"),
                generatedAt = json.optString("generatedAt", "unknown"),
                schema = json.optString("schema", "chargers_fast.geojson+operators.json")
            )
        } catch (_: Exception) {
            null
        }
    }

    private fun copyDocumentToFile(source: DocumentFile, target: File) {
        val resolver = context.contentResolver
        resolver.openInputStream(source.uri).use { input ->
            if (input == null) {
                throw IOException("Quelle konnte nicht gelesen werden: ${source.name}")
            }
            target.outputStream().use { output ->
                input.copyTo(output)
            }
        }
    }
}
