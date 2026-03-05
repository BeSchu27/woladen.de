package de.woladen.android.ui.components

import androidx.annotation.DrawableRes
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.outlined.Place
import androidx.compose.material3.Icon
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.res.painterResource
import de.woladen.android.R

@DrawableRes
fun amenityIconResIdOrNull(key: String): Int? {
    return when (key) {
        "amenity_restaurant" -> R.drawable.amenityicon_restaurant
        "amenity_cafe" -> R.drawable.amenityicon_cafe
        "amenity_fast_food" -> R.drawable.amenityicon_fast_food
        "amenity_toilets" -> R.drawable.amenityicon_toilets
        "amenity_supermarket" -> R.drawable.amenityicon_supermarket
        "amenity_bakery" -> R.drawable.amenityicon_bakery
        "amenity_convenience" -> R.drawable.amenityicon_convenience
        "amenity_pharmacy" -> R.drawable.amenityicon_pharmacy
        "amenity_hotel" -> R.drawable.amenityicon_hotel
        "amenity_museum" -> R.drawable.amenityicon_museum
        "amenity_playground" -> R.drawable.amenityicon_playground
        "amenity_park" -> R.drawable.amenityicon_park
        "amenity_ice_cream" -> R.drawable.amenityicon_ice_cream
        else -> null
    }
}

@Composable
fun AmenityIcon(
    key: String,
    contentDescription: String?,
    modifier: Modifier = Modifier
) {
    val resId = amenityIconResIdOrNull(key)
    if (resId != null) {
        Icon(
            painter = painterResource(id = resId),
            contentDescription = contentDescription,
            modifier = modifier,
            tint = Color.Unspecified
        )
    } else {
        Icon(
            imageVector = Icons.Outlined.Place,
            contentDescription = contentDescription,
            modifier = modifier
        )
    }
}
