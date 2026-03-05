package de.woladen.android

import android.Manifest
import android.os.SystemClock
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import androidx.test.rule.GrantPermissionRule
import androidx.test.uiautomator.By
import androidx.test.uiautomator.BySelector
import androidx.test.uiautomator.UiDevice
import androidx.test.uiautomator.UiObject2
import androidx.test.uiautomator.Until
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
class WoladenSmokeTest {

    @get:Rule(order = 0)
    val permissionRule: GrantPermissionRule = GrantPermissionRule.grant(
        Manifest.permission.ACCESS_FINE_LOCATION,
        Manifest.permission.ACCESS_COARSE_LOCATION
    )

    private val device: UiDevice = UiDevice.getInstance(InstrumentationRegistry.getInstrumentation())

    @Before
    fun launchApp() {
        dismissKeyguardAndSystemPanels()
        runCatching { device.executeShellCommand("am start -W -n $PACKAGE_NAME/.MainActivity") }
        waitByRes("tab-list", 30_000)
        device.waitForIdle()
    }

    @Test
    fun smoke_all_primary_features_including_station_detail_sheet() {
        waitByRes("station-row", 90_000)

        clickByRes("list-filter-button")
        waitByRes("filter-sheet", 20_000)
        clickByRes("filter-apply-button")

        clickByRes("tab-map")
        clickByRes("map-location-button")
        clickByRes("map-filter-button")
        waitByRes("filter-sheet", 20_000)
        clickByRes("filter-apply-button")

        clickByRes("tab-list")
        clickFirstByRes("station-row")

        waitByRes("station-detail-sheet", 20_000)
        waitByRes("detail-favorite-button", 10_000)
        waitByRes("detail-google-nav-button", 10_000)
        waitByRes("detail-system-nav-button", 10_000)
        waitByTextContains("In der Nähe", 10_000)
        waitByTextContains("Ladepunkte", 10_000)
        clickByRes("detail-favorite-button")
        clickByRes("detail-close-button")

        clickByRes("tab-favorites")
        waitByRes("favorites-row", 20_000)

        clickByRes("tab-info")
        waitByRes("info-root", 20_000)
        clickByRes("info-location-refresh-button")

        clickByRes("tab-list")
        waitByRes("station-row", 20_000)
    }

    @Test
    fun regression_repeated_map_taps_remain_responsive_and_detail_still_opens() {
        waitByRes("station-row", 90_000)
        clickByRes("tab-map")
        waitByRes("map-view-host", 20_000)

        repeat(8) {
            tapCenter()
            tapCenter()
            SystemClock.sleep(120)
        }

        clickByRes("map-filter-button")
        waitByRes("filter-sheet", 20_000)
        clickByRes("filter-apply-button")

        clickByRes("tab-list")
        waitByRes("station-row", 20_000)
        clickFirstByRes("station-row")
        waitByRes("station-detail-sheet", 20_000)
        clickByRes("detail-close-button")
    }

    @Test
    fun stationDetailSheet_actions_and_content_are_accessible() {
        waitByRes("station-row", 90_000)
        clickFirstByRes("station-row")

        waitByRes("station-detail-sheet", 20_000)
        waitByTextContains("In der Nähe", 10_000)
        waitByRes("detail-google-nav-button", 10_000)
        waitByRes("detail-system-nav-button", 10_000)
        clickByRes("detail-favorite-button")
        clickByRes("detail-close-button")
    }

    private fun clickByRes(tag: String) {
        waitByRes(tag, 20_000).click()
        device.waitForIdle()
    }

    private fun clickFirstByRes(tag: String) {
        val objects = device.findObjects(By.res(PACKAGE_NAME, tag))
        if (objects.isEmpty()) {
            throw AssertionError("No UI object found for tag=$tag")
        }
        objects.first().click()
        device.waitForIdle()
    }

    private fun tapCenter() {
        val x = device.displayWidth / 2
        val y = device.displayHeight / 2
        device.click(x, y)
        device.waitForIdle()
    }

    private fun waitByRes(tag: String, timeoutMs: Long): UiObject2 {
        return waitObject(By.res(PACKAGE_NAME, tag), timeoutMs)
    }

    private fun waitByTextContains(text: String, timeoutMs: Long): UiObject2 {
        return waitObject(By.textContains(text), timeoutMs)
    }

    private fun waitObject(selector: BySelector, timeoutMs: Long): UiObject2 {
        return device.wait(Until.findObject(selector), timeoutMs)
            ?: throw AssertionError("UI element not found for selector: $selector")
    }

    private fun dismissKeyguardAndSystemPanels() {
        runCatching { device.wakeUp() }
        runCatching { device.executeShellCommand("input keyevent KEYCODE_WAKEUP") }
        runCatching { device.executeShellCommand("wm dismiss-keyguard") }
        runCatching { device.executeShellCommand("input swipe 500 2200 500 350 250") }
        runCatching { device.executeShellCommand("input keyevent 82") }
        runCatching { device.executeShellCommand("input keyevent KEYCODE_MENU") }
        runCatching { device.executeShellCommand("cmd statusbar collapse") }
        runCatching { device.executeShellCommand("cmd statusbar collapse") }
    }

    companion object {
        private const val PACKAGE_NAME = "de.woladen.android"
    }
}
