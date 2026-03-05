package de.woladen.android.ui.components

import android.content.Context
import android.graphics.Bitmap
import android.graphics.Canvas
import android.graphics.Color
import android.graphics.Paint
import android.graphics.drawable.BitmapDrawable
import android.graphics.drawable.Drawable
import androidx.annotation.ColorInt

fun createCircleMarkerDrawable(
    context: Context,
    @ColorInt fillColor: Int,
    diameterDp: Float,
    strokeDp: Float = 1.5f,
    @ColorInt strokeColor: Int = Color.WHITE
): Drawable {
    val density = context.resources.displayMetrics.density
    val diameterPx = (diameterDp * density).toInt().coerceAtLeast(2)
    val strokePx = strokeDp * density
    val bitmap = Bitmap.createBitmap(diameterPx, diameterPx, Bitmap.Config.ARGB_8888)
    val canvas = Canvas(bitmap)

    val fillPaint = Paint(Paint.ANTI_ALIAS_FLAG).apply {
        color = fillColor
        style = Paint.Style.FILL
    }

    val strokePaint = Paint(Paint.ANTI_ALIAS_FLAG).apply {
        color = strokeColor
        style = Paint.Style.STROKE
        strokeWidth = strokePx
    }

    val center = diameterPx / 2f
    val radius = center - strokePx

    canvas.drawCircle(center, center, radius, fillPaint)
    canvas.drawCircle(center, center, radius, strokePaint)

    return BitmapDrawable(context.resources, bitmap)
}

fun markerColorForKey(key: String): Int {
    return when (key) {
        "gold" -> Color.rgb(255, 215, 0)
        "silver" -> Color.GRAY
        "bronze" -> Color.rgb(150, 75, 0)
        else -> Color.DKGRAY
    }
}
