package de.woladen.android.app

import android.app.Application
import org.maplibre.android.MapLibre

class WoladenApplication : Application() {
    override fun onCreate() {
        super.onCreate()
        MapLibre.getInstance(this)
    }
}
