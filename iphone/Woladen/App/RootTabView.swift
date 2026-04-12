import SwiftUI

struct RootTabView: View {
    @EnvironmentObject private var viewModel: AppViewModel
    @EnvironmentObject private var locationService: LocationService

    @State private var showingFilter = false

    var body: some View {
        currentTab
            .frame(maxWidth: .infinity, maxHeight: .infinity)
            .background(Color(.systemBackground))
            .safeAreaInset(edge: .bottom, spacing: 0) {
                tabBar
            }
        .ignoresSafeArea(.keyboard, edges: .bottom)
        .sheet(isPresented: $showingFilter) {
            FilterSheetView(
                filter: viewModel.filterState,
                operators: viewModel.operators,
                availableAmenityKeys: availableAmenityKeys()
            ) { newFilter in
                viewModel.filterState = newFilter
                viewModel.applyFilters(userLocation: locationService.currentLocation)
            }
            .presentationDetents([.medium, .large])
        }
        .sheet(item: $viewModel.selectedFeature) { feature in
            StationDetailView(feature: feature)
                .presentationDetents([.large])
                .presentationDragIndicator(.visible)
        }
    }

    @ViewBuilder
    private var currentTab: some View {
        switch viewModel.selectedTab {
        case .list:
            ListTabView(showingFilter: $showingFilter)
        case .map:
            MapTabView(showingFilter: $showingFilter)
        case .favorites:
            FavoritesTabView()
        case .info:
            InfoTabView()
        }
    }

    private var tabBar: some View {
        VStack(spacing: 8) {
            HStack(spacing: 8) {
                tabButton(.list, title: "Liste", systemImage: "list.bullet")
                tabButton(.map, title: "Karte", systemImage: "map")
                tabButton(.favorites, title: "Favoriten", systemImage: "star")
                tabButton(.info, title: "Info", systemImage: "info.circle")
            }
            .padding(.horizontal, 8)
            .padding(.vertical, 8)
            .background(.ultraThinMaterial, in: Capsule())
            .overlay {
                Capsule()
                    .stroke(Color.primary.opacity(0.08), lineWidth: 0.5)
            }

            Capsule()
                .fill(Color.secondary.opacity(0.35))
                .frame(width: 120, height: 5)
        }
        .frame(maxWidth: 380)
        .padding(.horizontal, 16)
        .padding(.top, 6)
        .padding(.bottom, 8)
        .frame(maxWidth: .infinity)
    }

    private func tabButton(_ tab: AppViewModel.AppTab, title: String, systemImage: String) -> some View {
        let isSelected = viewModel.selectedTab == tab
        return Button {
            viewModel.selectedTab = tab
        } label: {
            VStack(spacing: 4) {
                Image(systemName: systemImage)
                    .font(.system(size: 17, weight: .semibold))
                Text(title)
                    .font(.system(size: 11, weight: .semibold))
            }
            .frame(maxWidth: .infinity, minHeight: 54)
            .foregroundStyle(isSelected ? Color.accentColor : Color.secondary)
            .background {
                RoundedRectangle(cornerRadius: 18, style: .continuous)
                    .fill(isSelected ? Color.accentColor.opacity(0.14) : Color.clear)
            }
        }
        .buttonStyle(.plain)
    }

    private func availableAmenityKeys() -> [String] {
        var keys = Set<String>()
        for feature in viewModel.allFeatures {
            for (key, count) in feature.properties.amenityCounts where count > 0 {
                keys.insert(key)
            }
        }
        return keys.sorted { AmenityCatalog.label(for: $0) < AmenityCatalog.label(for: $1) }
    }
}
