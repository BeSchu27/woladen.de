package de.woladen.android.store

import android.content.Context
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.setValue

class FavoritesStore(context: Context) {
    private val preferences = context.getSharedPreferences("woladen", Context.MODE_PRIVATE)
    private val defaultsKey = "woladen_favorites"

    var favorites: Set<String> by mutableStateOf(loadFavorites())
        private set

    fun toggle(stationId: String) {
        val next = favorites.toMutableSet()
        if (next.contains(stationId)) {
            next.remove(stationId)
        } else {
            next.add(stationId)
        }
        favorites = next
        saveFavorites()
    }

    fun remove(stationId: String) {
        if (!favorites.contains(stationId)) return
        val next = favorites.toMutableSet()
        next.remove(stationId)
        favorites = next
        saveFavorites()
    }

    fun isFavorite(stationId: String): Boolean {
        return favorites.contains(stationId)
    }

    private fun loadFavorites(): Set<String> {
        return preferences.getStringSet(defaultsKey, emptySet()).orEmpty().toSet()
    }

    private fun saveFavorites() {
        preferences.edit().putStringSet(defaultsKey, favorites).apply()
    }
}
