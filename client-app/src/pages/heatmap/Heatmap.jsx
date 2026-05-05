import { useEffect, useMemo, useRef, useState } from 'react'
import { motion } from 'framer-motion'
import {
  AlertTriangle,
  Building2,
  CheckCircle2,
  Crosshair,
  History,
  LoaderCircle,
  MapPin,
  Radar,
  Search,
  Trash2,
  Wrench,
} from 'lucide-react'
import Card, { CardHeader } from '../../components/ui/Card'
import { api } from '../../lib/api'
import styles from './Heatmap.module.css'

const MotionDiv = motion.div

const gridOptions = [3, 5, 7, 9]
const radiusOptions = [1, 3, 5, 10, 15]
const MILES_TO_METERS = 1609.34

let googleMapsPromise

function sleep(ms) {
  return new Promise((resolve) => {
    window.setTimeout(resolve, ms)
  })
}

function loadGoogleMapsApi(apiKey) {
  if (!apiKey) {
    return Promise.reject(new Error('Google Maps API key is missing.'))
  }

  if (window.google?.maps) {
    return Promise.resolve(window.google.maps)
  }

  if (googleMapsPromise) {
    return googleMapsPromise
  }

  googleMapsPromise = new Promise((resolve, reject) => {
    const callbackName = `__gromoreHeatmapInit${Date.now()}`
    const script = document.createElement('script')
    script.id = 'gromore-google-maps-script'
    script.async = true
    script.defer = true
    script.src = `https://maps.googleapis.com/maps/api/js?key=${encodeURIComponent(apiKey)}&libraries=places&callback=${callbackName}&loading=async`
    script.onerror = () => reject(new Error('Google Maps failed to load.'))
    window[callbackName] = () => {
      delete window[callbackName]
      resolve(window.google.maps)
    }
    document.head.appendChild(script)
  })

  return googleMapsPromise
}

function formatCoords(lat, lng) {
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) return 'Not set'
  return `${lat.toFixed(4)}, ${lng.toFixed(4)}`
}

function distanceMiles(lat1, lng1, lat2, lng2) {
  if (![lat1, lng1, lat2, lng2].every(Number.isFinite)) return 0
  const toRad = Math.PI / 180
  const dLat = (lat2 - lat1) * toRad
  const dLng = (lng2 - lng1) * toRad
  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(lat1 * toRad) * Math.cos(lat2 * toRad) *
    Math.sin(dLng / 2) * Math.sin(dLng / 2)
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
  return 3958.8 * c
}

function buildGridPoints(centerLat, centerLng, radiusMiles, gridSize) {
  const radiusKm = radiusMiles * 1.60934
  const half = (gridSize - 1) / 2
  const kmPerDegLat = 111.32
  const kmPerDegLng = kmPerDegLat * Math.cos((centerLat * Math.PI) / 180)
  const stepKm = gridSize > 1 ? (2 * radiusKm) / (gridSize - 1) : 0
  const points = []

  for (let row = 0; row < gridSize; row += 1) {
    for (let col = 0; col < gridSize; col += 1) {
      const dLat = ((half - row) * stepKm) / kmPerDegLat
      const dLng = kmPerDegLng ? ((col - half) * stepKm) / kmPerDegLng : 0
      points.push({
        row,
        col,
        lat: Number((centerLat + dLat).toFixed(6)),
        lng: Number((centerLng + dLng).toFixed(6)),
      })
    }
  }

  return points
}

function closestCellIndex(points, targetLat, targetLng) {
  let bestIndex = -1
  let bestDistance = Number.POSITIVE_INFINITY

  points.forEach((point, index) => {
    const deltaLat = point.lat - targetLat
    const deltaLng = point.lng - targetLng
    const score = (deltaLat * deltaLat) + (deltaLng * deltaLng)
    if (score < bestDistance) {
      bestDistance = score
      bestIndex = index
    }
  })

  return bestIndex
}

function rankColor(rank) {
  if (rank >= 1 && rank <= 3) return '#22c55e'
  if (rank >= 4 && rank <= 10) return '#f59e0b'
  if (rank > 10) return '#ef4444'
  return '#94a3b8'
}

function summarizeApiChecks(checks) {
  if (!checks) return null
  return [
    `Browser scanner: ${checks.browser_runtime?.detail || 'Unknown'}`,
    `Places API (New): ${checks.places_new?.detail || 'Unknown'}`,
    `Places API (Legacy): ${checks.places_legacy?.detail || 'Unknown'}`,
    `Geocoding API: ${checks.geocoding?.detail || 'Unknown'}`,
  ]
}

export default function Heatmap() {
  const [bootLoading, setBootLoading] = useState(true)
  const [pageError, setPageError] = useState('')
  const [brand, setBrand] = useState(null)
  const [history, setHistory] = useState([])
  const [activeScanId, setActiveScanId] = useState(null)
  const [keyword, setKeyword] = useState('')
  const [radiusMiles, setRadiusMiles] = useState(5)
  const [gridSize, setGridSize] = useState(5)
  const [center, setCenter] = useState(null)
  const [results, setResults] = useState([])
  const [competitorSummary, setCompetitorSummary] = useState([])
  const [avgRank, setAvgRank] = useState(0)
  const [debugInfo, setDebugInfo] = useState(null)
  const [selectedCellIndex, setSelectedCellIndex] = useState(-1)
  const [scanDirty, setScanDirty] = useState(false)
  const [scanBusy, setScanBusy] = useState(false)
  const [saveLocationBusy, setSaveLocationBusy] = useState(false)
  const [locationInput, setLocationInput] = useState('')
  const [locationMessage, setLocationMessage] = useState('')
  const [centerSearch, setCenterSearch] = useState('')
  const [centerMessage, setCenterMessage] = useState('')
  const [placeSearchQuery, setPlaceSearchQuery] = useState('')
  const [placeSearchBusy, setPlaceSearchBusy] = useState(false)
  const [placeSearchResults, setPlaceSearchResults] = useState([])
  const [manualPlaceId, setManualPlaceId] = useState('')
  const [placeMessage, setPlaceMessage] = useState('')
  const [apiTestBusy, setApiTestBusy] = useState(false)
  const [apiTestLines, setApiTestLines] = useState([])

  const mapNodeRef = useRef(null)
  const mapRef = useRef(null)
  const businessMarkerRef = useRef(null)
  const scanMarkerRef = useRef(null)
  const resultMarkersRef = useRef([])
  const previewDotsRef = useRef([])
  const previewLinesRef = useRef([])
  const previewCircleRef = useRef(null)
  const geocoderRef = useRef(null)
  const centerSearchInputRef = useRef(null)
  const locationInputRef = useRef(null)
  const centerAutocompleteRef = useRef(null)
  const locationAutocompleteRef = useRef(null)
  const mapsReadyRef = useRef(false)

  const hasApiKey = Boolean(brand?.google_maps_api_key)
  const hasLocation = Boolean(brand?.business_lat && brand?.business_lng)
  const rankedCount = useMemo(() => results.filter(cell => cell.rank > 0).length, [results])
  const top3Count = useMemo(() => results.filter(cell => cell.rank >= 1 && cell.rank <= 3).length, [results])
  const distanceFromBusiness = center && brand
    ? distanceMiles(brand.business_lat, brand.business_lng, center.lat, center.lng)
    : 0
  const gridPointCount = gridSize * gridSize
  const browserLookupEstimate = gridPointCount
  const centerCellIndex = center ? closestCellIndex(results, center.lat, center.lng) : -1
  const selectedCell = selectedCellIndex >= 0 ? results[selectedCellIndex] : null

  const applyLoadedScan = (scan) => {
    setActiveScanId(scan.id)
    setKeyword(scan.keyword || '')
    setRadiusMiles(scan.radius_miles || 5)
    setGridSize(scan.grid_size || 5)
    setCenter({ lat: scan.center_lat, lng: scan.center_lng })
    setResults(scan.results || [])
    setCompetitorSummary(scan.competitor_summary || [])
    setAvgRank(scan.avg_rank || 0)
    setSelectedCellIndex(-1)
    setDebugInfo(scan.debug || null)
    setScanDirty(false)
  }

  const waitForScanCompletion = async (scanId) => {
    for (let attempt = 0; attempt < 150; attempt += 1) {
      const data = await api.get(`/heatmap/scan/${scanId}`)
      if (!data.ok) throw new Error(data.error || 'Failed to load scan.')
      const scan = data.scan
      if (scan.status === 'complete') return scan
      if (scan.status === 'failed') throw new Error(scan.error_message || 'Heatmap scan failed.')
      await sleep(2000)
    }
    throw new Error('Heatmap scan is still running. Refresh in a moment.')
  }

  const hydrateState = (payload) => {
    const nextBrand = payload.brand
    const nextHistory = payload.scans || []
    const active = payload.active_scan || null

    setBrand(nextBrand)
    setHistory(nextHistory)
    setKeyword(active?.keyword || '')
    setRadiusMiles(active?.radius_miles || 5)
    setGridSize(active?.grid_size || 5)
    setCenter({
      lat: active?.center_lat || nextBrand.business_lat || 0,
      lng: active?.center_lng || nextBrand.business_lng || 0,
    })
    setResults(active?.results || [])
    setCompetitorSummary(active?.competitor_summary || [])
    setAvgRank(active?.avg_rank || 0)
    setActiveScanId(active?.id || null)
    setSelectedCellIndex(-1)
    setDebugInfo(null)
    setScanDirty(false)
    setPlaceSearchQuery(nextBrand.display_name || '')
    setManualPlaceId(nextBrand.google_place_id || '')
    setCenterMessage('')
    setLocationMessage('')
    setPlaceMessage('')
    setApiTestLines([])
  }

  useEffect(() => {
    if (!results.length) {
      setSelectedCellIndex(-1)
      return
    }

    if (selectedCellIndex >= 0 && selectedCellIndex < results.length) {
      return
    }

    const fallbackIndex = centerCellIndex >= 0 ? centerCellIndex : 0
    setSelectedCellIndex(fallbackIndex)
  }, [centerCellIndex, results, selectedCellIndex])

  const loadHeatmap = async () => {
    setPageError('')
    const data = await api.get('/api/heatmap')
    if (!data.ok) {
      throw new Error(data.error || 'Failed to load heatmap.')
    }
    hydrateState(data)
    return data
  }

  useEffect(() => {
    let ignore = false

    const run = async () => {
      try {
        setBootLoading(true)
        await loadHeatmap()
        if (ignore) return
      } catch (error) {
        if (!ignore) {
          setPageError(error.message || 'Failed to load heatmap.')
        }
      } finally {
        if (!ignore) setBootLoading(false)
      }
    }

    run()
    return () => {
      ignore = true
    }
  }, [])

  useEffect(() => {
    if (!hasApiKey || !mapNodeRef.current || !center) return undefined
    let cancelled = false

    loadGoogleMapsApi(brand.google_maps_api_key)
      .then(() => {
        if (cancelled || mapRef.current || !mapNodeRef.current) return

        const map = new window.google.maps.Map(mapNodeRef.current, {
          center,
          zoom: 11,
          mapTypeControl: false,
          streetViewControl: false,
          fullscreenControl: true,
        })

        mapRef.current = map
        geocoderRef.current = new window.google.maps.Geocoder()
        mapsReadyRef.current = true

        if (hasLocation) {
          businessMarkerRef.current = new window.google.maps.Marker({
            position: { lat: brand.business_lat, lng: brand.business_lng },
            map,
            title: 'Business location',
            icon: {
              path: window.google.maps.SymbolPath.CIRCLE,
              scale: 9,
              fillColor: '#4f46e5',
              fillOpacity: 1,
              strokeColor: '#ffffff',
              strokeWeight: 3,
            },
            zIndex: 90,
          })
        }

        scanMarkerRef.current = new window.google.maps.Marker({
          position: center,
          map,
          draggable: true,
          title: 'Scan center',
        })

        scanMarkerRef.current.addListener('dragend', (event) => {
          setCenter({ lat: event.latLng.lat(), lng: event.latLng.lng() })
          setScanDirty(true)
        })

        map.addListener('click', (event) => {
          setCenter({ lat: event.latLng.lat(), lng: event.latLng.lng() })
          setScanDirty(true)
        })
      })
      .catch((error) => {
        if (!cancelled) {
          setPageError(error.message || 'Google Maps failed to load.')
        }
      })

    return () => {
      cancelled = true
    }
  }, [brand, center, hasApiKey, hasLocation])

  useEffect(() => {
    if (!mapsReadyRef.current || !mapRef.current || !center) return

    if (scanMarkerRef.current) {
      scanMarkerRef.current.setPosition(center)
    }
    mapRef.current.panTo(center)
  }, [center])

  useEffect(() => {
    if (!mapsReadyRef.current || !mapRef.current || !center) return

    previewDotsRef.current.forEach(dot => dot.setMap(null))
    previewLinesRef.current.forEach(line => line.setMap(null))
    resultMarkersRef.current.forEach(marker => marker.setMap(null))
    previewDotsRef.current = []
    previewLinesRef.current = []
    resultMarkersRef.current = []

    if (previewCircleRef.current) {
      previewCircleRef.current.setMap(null)
      previewCircleRef.current = null
    }

    const points = buildGridPoints(center.lat, center.lng, radiusMiles, gridSize)
    const centerIndex = closestCellIndex(points, center.lat, center.lng)

    previewCircleRef.current = new window.google.maps.Circle({
      map: mapRef.current,
      center,
      radius: radiusMiles * MILES_TO_METERS,
      strokeColor: '#2563eb',
      strokeOpacity: 0.35,
      strokeWeight: 2,
      fillColor: '#2563eb',
      fillOpacity: 0.05,
      clickable: false,
    })

    for (let row = 0; row < gridSize; row += 1) {
      const rowPath = []
      for (let col = 0; col < gridSize; col += 1) {
        rowPath.push(points[(row * gridSize) + col])
      }
      previewLinesRef.current.push(new window.google.maps.Polyline({
        map: mapRef.current,
        path: rowPath,
        strokeColor: '#94a3b8',
        strokeOpacity: 0.5,
        strokeWeight: 1,
        clickable: false,
      }))
    }

    for (let col = 0; col < gridSize; col += 1) {
      const colPath = []
      for (let row = 0; row < gridSize; row += 1) {
        colPath.push(points[(row * gridSize) + col])
      }
      previewLinesRef.current.push(new window.google.maps.Polyline({
        map: mapRef.current,
        path: colPath,
        strokeColor: '#94a3b8',
        strokeOpacity: 0.5,
        strokeWeight: 1,
        clickable: false,
      }))
    }

    points.forEach((point, index) => {
      previewDotsRef.current.push(new window.google.maps.Circle({
        map: mapRef.current,
        center: point,
        radius: index === centerIndex ? 160 : 110,
        strokeOpacity: 0,
        fillColor: index === centerIndex ? '#4f46e5' : '#64748b',
        fillOpacity: index === centerIndex ? 0.8 : 0.25,
        clickable: false,
      }))
    })

    results.forEach((cell) => {
      resultMarkersRef.current.push(new window.google.maps.Marker({
        position: { lat: cell.lat, lng: cell.lng },
        map: mapRef.current,
        title: `Rank ${cell.rank > 0 ? cell.rank : 'Not found'}`,
        icon: {
          path: window.google.maps.SymbolPath.CIRCLE,
          scale: 18,
          fillColor: rankColor(cell.rank),
          fillOpacity: 0.88,
          strokeColor: '#ffffff',
          strokeWeight: 2,
        },
        label: {
          text: cell.rank > 0 ? String(cell.rank) : '-',
          color: '#ffffff',
          fontSize: '11px',
          fontWeight: '700',
        },
      }))
    })
  }, [center, gridSize, radiusMiles, results])

  useEffect(() => {
    if (!mapsReadyRef.current || !window.google?.maps?.places || !centerSearchInputRef.current || centerAutocompleteRef.current) {
      return
    }

    centerAutocompleteRef.current = new window.google.maps.places.Autocomplete(centerSearchInputRef.current, {
      fields: ['formatted_address', 'geometry', 'name'],
    })
    centerAutocompleteRef.current.addListener('place_changed', () => {
      const place = centerAutocompleteRef.current.getPlace()
      if (!place?.geometry?.location) return
      setCenterSearch(place.formatted_address || place.name || '')
      setCenter({ lat: place.geometry.location.lat(), lng: place.geometry.location.lng() })
      setCenterMessage(`Scan center moved to ${place.formatted_address || place.name}.`)
      setScanDirty(true)
    })
  }, [center])

  useEffect(() => {
    if (!mapsReadyRef.current || !window.google?.maps?.places || !locationInputRef.current || locationAutocompleteRef.current) {
      return
    }

    locationAutocompleteRef.current = new window.google.maps.places.Autocomplete(locationInputRef.current, {
      fields: ['formatted_address', 'geometry', 'name'],
    })
    locationAutocompleteRef.current.addListener('place_changed', () => {
      const place = locationAutocompleteRef.current.getPlace()
      if (!place) return
      setLocationInput(place.formatted_address || place.name || '')
    })
  }, [locationInput])

  const handleScan = async () => {
    if (!keyword.trim()) {
      setPageError('Enter a search term first.')
      return
    }
    if (!center) {
      setPageError('Move the scan center before running the heatmap.')
      return
    }

    try {
      setPageError('')
      setScanBusy(true)
      const data = await api.post('/heatmap/scan', {
        keyword: keyword.trim(),
        radius_miles: radiusMiles,
        grid_size: gridSize,
        center_lat: center.lat,
        center_lng: center.lng,
      })
      if (!data.ok) throw new Error(data.error || 'Heatmap scan failed.')

      const newHistoryItem = {
        id: data.scan_id,
        keyword: keyword.trim(),
        radius_miles: radiusMiles,
        grid_size: gridSize,
        avg_rank: data.avg_rank,
        center_lat: data.center_lat,
        center_lng: data.center_lng,
        status: data.status || 'complete',
        scanned_at: 'Just now',
      }

      setHistory((prev) => [newHistoryItem, ...prev.filter(scan => scan.id !== data.scan_id)])
      setActiveScanId(data.scan_id)

      if (data.pending) {
        const completedScan = await waitForScanCompletion(data.scan_id)
        applyLoadedScan(completedScan)
        setHistory((prev) => prev.map((scan) => (
          scan.id === completedScan.id
            ? {
                ...scan,
                avg_rank: completedScan.avg_rank,
                status: completedScan.status || 'complete',
                scanned_at: completedScan.scanned_at || scan.scanned_at,
              }
            : scan
        )))
        return
      }

      setResults(data.results || [])
      setCompetitorSummary(data.competitor_summary || [])
      setAvgRank(data.avg_rank || 0)
      setSelectedCellIndex(-1)
      setDebugInfo(data.debug || null)
      setScanDirty(false)
      setCenter({ lat: data.center_lat, lng: data.center_lng })
    } catch (error) {
      setPageError(error.message || 'Heatmap scan failed.')
    } finally {
      setScanBusy(false)
    }
  }

  const handleLoadScan = async (scanId) => {
    try {
      setPageError('')
      const data = await api.get(`/heatmap/scan/${scanId}`)
      if (!data.ok) throw new Error(data.error || 'Failed to load scan.')
      const scan = data.scan.status === 'pending' ? await waitForScanCompletion(scanId) : data.scan
      applyLoadedScan(scan)
    } catch (error) {
      setPageError(error.message || 'Failed to load scan.')
    }
  }

  const handleDeleteScan = async (scanId) => {
    if (!window.confirm('Delete this scan?')) return

    try {
      const data = await api.del(`/heatmap/scan/${scanId}`)
      if (!data.ok) throw new Error(data.error || 'Failed to delete scan.')
      const nextHistory = history.filter(scan => scan.id !== scanId)
      setHistory(nextHistory)
      if (activeScanId === scanId) {
        if (nextHistory[0]) {
          handleLoadScan(nextHistory[0].id)
        } else {
          setActiveScanId(null)
          setResults([])
          setCompetitorSummary([])
          setAvgRank(0)
          setSelectedCellIndex(-1)
          setDebugInfo(null)
        }
      }
    } catch (error) {
      setPageError(error.message || 'Failed to delete scan.')
    }
  }

  const handleClearScans = async () => {
    if (!window.confirm('Delete all scan history?')) return
    try {
      const data = await api.del('/heatmap/scans')
      if (!data.ok) throw new Error(data.error || 'Failed to clear scans.')
      setHistory([])
      setActiveScanId(null)
      setResults([])
      setCompetitorSummary([])
      setAvgRank(0)
      setSelectedCellIndex(-1)
      setDebugInfo(null)
    } catch (error) {
      setPageError(error.message || 'Failed to clear scan history.')
    }
  }

  const handleSaveBusinessLocation = async () => {
    if (!locationInput.trim()) {
      setLocationMessage('Enter an address or pick a place first.')
      return
    }

    try {
      setSaveLocationBusy(true)
      setLocationMessage('')
      const data = await api.post('/heatmap/save-location', { address: locationInput.trim() })
      if (!data.ok) throw new Error(data.error || 'Failed to save business location.')
      setLocationMessage(`Business location saved: ${data.formatted}`)
      const payload = await loadHeatmap()
      setCenter({
        lat: payload.active_scan?.center_lat || payload.brand.business_lat,
        lng: payload.active_scan?.center_lng || payload.brand.business_lng,
      })
    } catch (error) {
      setLocationMessage(error.message || 'Failed to save business location.')
    } finally {
      setSaveLocationBusy(false)
    }
  }

  const handleCenterSearch = async () => {
    if (!centerSearch.trim()) {
      setCenterMessage('Enter an address or place to move the scan center.')
      return
    }

    if (!geocoderRef.current) {
      setCenterMessage('Map search is still loading.')
      return
    }

    geocoderRef.current.geocode({ address: centerSearch.trim() }, (matches, status) => {
      if (status !== 'OK' || !matches?.length) {
        setCenterMessage('Could not place that location on the map.')
        return
      }
      const match = matches[0]
      const location = match.geometry.location
      setCenter({ lat: location.lat(), lng: location.lng() })
      setCenterMessage(`Scan center moved to ${match.formatted_address}.`)
      setScanDirty(true)
    })
  }

  const handlePlaceSearch = async () => {
    if (!placeSearchQuery.trim()) {
      setPlaceMessage('Enter a business name or Place ID.')
      return
    }

    try {
      setPlaceSearchBusy(true)
      setPlaceMessage('')
      setPlaceSearchResults([])
      const data = await api.post('/settings/search-place', { query: placeSearchQuery.trim() })
      if (!data.ok) throw new Error(data.error || 'Place search failed.')
      setPlaceSearchResults(data.results || [])
    } catch (error) {
      setPlaceMessage(error.message || 'Place search failed.')
    } finally {
      setPlaceSearchBusy(false)
    }
  }

  const savePlaceId = async (placeId) => {
    try {
      const data = await api.post('/settings/save-place-id', { place_id: placeId })
      if (!data.ok) throw new Error(data.error || 'Failed to save Place ID.')
      setBrand(prev => ({ ...prev, google_place_id: placeId }))
      setManualPlaceId(placeId)
      setPlaceMessage('Place ID saved.')
    } catch (error) {
      setPlaceMessage(error.message || 'Failed to save Place ID.')
    }
  }

  const handleTestApi = async () => {
    try {
      setApiTestBusy(true)
      const data = await api.post('/heatmap/test-api', {})
      if (!data.ok) throw new Error(data.error || 'API test failed.')
      setApiTestLines(summarizeApiChecks(data.checks) || [])
    } catch (error) {
      setApiTestLines([error.message || 'API test failed.'])
    } finally {
      setApiTestBusy(false)
    }
  }

  if (bootLoading) {
    return (
      <div className={styles.loadingState}>
        <LoaderCircle size={20} className={styles.spinning} />
        Loading heatmap...
      </div>
    )
  }

  return (
    <MotionDiv initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }} className={styles.page}>
      <div className={styles.hero}>
        <div>
          <h1 className={styles.title}>Rank Heatmap</h1>
          <p className={styles.subtitle}>Search a service term, move the scan center, and map Google Maps local-pack visibility across the grid. This does not track your website&apos;s organic page-one ranking.</p>
        </div>
        <div className={styles.heroPills}>
          <span className={styles.heroPill}><MapPin size={14} /> {formatCoords(center?.lat, center?.lng)}</span>
          <span className={styles.heroPill}><Crosshair size={14} /> {distanceFromBusiness < 0.1 ? '< 0.1 mi' : `${distanceFromBusiness.toFixed(1)} mi`} from business</span>
        </div>
      </div>

      {pageError && (
        <div className={styles.errorBanner}>
          <AlertTriangle size={16} />
          {pageError}
        </div>
      )}

      {!hasApiKey && (
        <Card className={styles.setupCard}>
          <CardHeader title="Google Maps API key required" subtitle="The heatmap needs Places API and Geocoding API access before anything else can run." />
          <p className={styles.setupText}>Your API key is still managed on the legacy Connections page.</p>
          <a className={styles.primaryLink} href="/client/settings">Open Connections</a>
        </Card>
      )}

      {hasApiKey && (
        <div className={styles.layout}>
          <div className={styles.mainColumn}>
            <Card className={styles.controlCard}>
              <div className={styles.formGrid}>
                <label className={styles.field}>
                  <span>Search term</span>
                  <input value={keyword} onChange={(event) => setKeyword(event.target.value)} placeholder="pooper scooper, plumber, lawn care" />
                </label>

                <label className={styles.field}>
                  <span>Radius</span>
                  <select value={radiusMiles} onChange={(event) => { setRadiusMiles(Number(event.target.value)); setScanDirty(true) }}>
                    {radiusOptions.map(option => <option key={option} value={option}>{option} mi</option>)}
                  </select>
                </label>

                <label className={styles.field}>
                  <span>Grid</span>
                  <select value={gridSize} onChange={(event) => { setGridSize(Number(event.target.value)); setScanDirty(true) }}>
                    {gridOptions.map(option => <option key={option} value={option}>{option} x {option}</option>)}
                    {!gridOptions.includes(gridSize) && <option value={gridSize}>{gridSize} x {gridSize} (legacy)</option>}
                  </select>
                </label>

                <button className={styles.scanButton} type="button" onClick={handleScan} disabled={scanBusy || !hasLocation}>
                  {scanBusy ? <LoaderCircle size={16} className={styles.spinning} /> : <Radar size={16} />}
                  Run Heatmap
                </button>
              </div>

              <div className={styles.metaRow}>
                <span className={styles.metaPill}>Grid points: {gridPointCount}</span>
                <span className={styles.metaPill}>Estimated Google Maps lookups: {browserLookupEstimate}</span>
                <span className={`${styles.metaPill} ${scanDirty ? styles.metaPillWarning : ''}`}>
                  {scanDirty ? 'Scan settings changed - rerun to refresh results' : 'Results match the current center'}
                </span>
              </div>

              <div className={styles.searchRow}>
                <label className={styles.fieldGrow}>
                  <span>Jump scan center to an address or place</span>
                  <input
                    ref={centerSearchInputRef}
                    value={centerSearch}
                    onChange={(event) => setCenterSearch(event.target.value)}
                    placeholder="Mesa, AZ or a neighborhood, landmark, or address"
                  />
                </label>
                <button className={styles.secondaryButton} type="button" onClick={handleCenterSearch}>
                  <Search size={15} />
                  Center map
                </button>
                <button
                  className={styles.secondaryButton}
                  type="button"
                  onClick={() => {
                    if (!brand) return
                    setCenter({ lat: brand.business_lat, lng: brand.business_lng })
                    setScanDirty(true)
                    setCenterMessage('Scan center reset to the business location.')
                  }}
                  disabled={!hasLocation}
                >
                  <Building2 size={15} />
                  Use business location
                </button>
              </div>

              {centerMessage && <p className={styles.inlineMessage}>{centerMessage}</p>}
            </Card>

            {hasLocation ? (
              <Card className={styles.mapCard} padding={false}>
                <div className={styles.mapHeader}>
                  <div>
                    <h2>Live scan map</h2>
                    <p>Drag the center pin or click the map to move the grid before you run the search.</p>
                  </div>
                  <div className={styles.mapHeaderMeta}>
                    <span>{gridSize} x {gridSize}</span>
                    <span>{radiusMiles} mi radius</span>
                  </div>
                </div>
                <div ref={mapNodeRef} className={styles.mapCanvas} />
              </Card>
            ) : (
              <Card>
                <CardHeader title="Set your business location" subtitle="Save a real business address first so the heatmap has a reliable anchor point." />
                <div className={styles.searchRow}>
                  <label className={styles.fieldGrow}>
                    <span>Business address</span>
                    <input
                      ref={locationInputRef}
                      value={locationInput}
                      onChange={(event) => setLocationInput(event.target.value)}
                      placeholder="123 Main St, Phoenix, AZ 85001"
                    />
                  </label>
                  <button className={styles.scanButton} type="button" onClick={handleSaveBusinessLocation} disabled={saveLocationBusy}>
                    {saveLocationBusy ? <LoaderCircle size={16} className={styles.spinning} /> : <MapPin size={16} />}
                    Save location
                  </button>
                </div>
                {locationMessage && <p className={styles.inlineMessage}>{locationMessage}</p>}
              </Card>
            )}

            {hasLocation && (
              <Card>
                <CardHeader title="Heatmap grid" subtitle={results.length ? `${radiusMiles} mile radius | ${gridSize} x ${gridSize} local-pack grid` : 'Run a scan to populate the local-pack grid.'} />
                {results.length ? (
                  <>
                    <div className={styles.statsRow}>
                      <span className={styles.statPill}>Avg local rank: {avgRank || 'N/A'}</span>
                      <span className={styles.statPill}>Found: {rankedCount} / {results.length}</span>
                      <span className={styles.statPill}>Top 3: {top3Count}</span>
                    </div>
                    <div className={styles.heatmapGrid} style={{ gridTemplateColumns: `repeat(${gridSize}, 1fr)` }}>
                      {results.map((cell, index) => {
                        const classes = [styles.heatCell]
                        if (cell.rank >= 1 && cell.rank <= 3) classes.push(styles.topRank)
                        else if (cell.rank >= 4 && cell.rank <= 10) classes.push(styles.midRank)
                        else if (cell.rank > 10) classes.push(styles.lowRank)
                        else classes.push(styles.noRank)
                        if (index === centerCellIndex) classes.push(styles.centerCell)
                        if (index === selectedCellIndex) classes.push(styles.selectedCell)

                        return (
                          <button
                            key={`${cell.row}-${cell.col}`}
                            type="button"
                            className={classes.join(' ')}
                            title={`Rank ${cell.rank > 0 ? cell.rank : 'Not found'} at ${formatCoords(cell.lat, cell.lng)}`}
                            onClick={() => setSelectedCellIndex(index)}
                          >
                            {cell.rank > 0 ? cell.rank : '-'}
                          </button>
                        )
                      })}
                    </div>
                    <div className={styles.legend}>
                      <span><i className={`${styles.legendDot} ${styles.legendTop}`} /> #1-3</span>
                      <span><i className={`${styles.legendDot} ${styles.legendMid}`} /> #4-10 local pack</span>
                      <span><i className={`${styles.legendDot} ${styles.legendLow}`} /> #11+ local pack</span>
                      <span><i className={`${styles.legendDot} ${styles.legendNone}`} /> Not returned in scan results</span>
                      <span><i className={`${styles.legendDot} ${styles.legendCenter}`} /> Closest cell to center</span>
                    </div>
                    <p className={styles.inlineMessage}>This heatmap measures Google Maps visibility from live browser results, with Places used only as a fallback when needed. Service-area businesses can rank on page one organically while still appearing inconsistently in map results.</p>
                  </>
                ) : (
                  <div className={styles.emptyState}>No scan yet. Set the term, move the center, and run the heatmap.</div>
                )}
              </Card>
            )}

            {hasLocation && results.length > 0 && (
              <div className={styles.competitorPanels}>
                <Card>
                  <CardHeader
                    title="Selected grid point"
                    subtitle={selectedCell ? `${formatCoords(selectedCell.lat, selectedCell.lng)} | ${selectedCell.competitors?.length || 0} competitors returned` : 'Pick a cell to inspect the local pack.'}
                  />
                  {selectedCell ? (
                    <>
                      <div className={styles.statsRow}>
                        <span className={styles.statPill}>Your rank: {selectedCell.rank > 0 ? `#${selectedCell.rank}` : 'Not found'}</span>
                        <span className={styles.statPill}>Cell: Row {selectedCell.row + 1}, Col {selectedCell.col + 1}</span>
                      </div>
                      <div className={styles.competitorList}>
                        {(selectedCell.competitors || []).length ? selectedCell.competitors.map((competitor) => (
                          <div key={`${selectedCell.row}-${selectedCell.col}-${competitor.rank}-${competitor.place_id || competitor.name}`} className={styles.competitorRow}>
                            <div className={styles.competitorRank}>#{competitor.rank}</div>
                            <div className={styles.competitorCopy}>
                              <strong>{competitor.name || 'Unknown business'}</strong>
                              <span>{competitor.address || 'Address unavailable'}</span>
                            </div>
                            {competitor.is_target && <span className={styles.targetBadge}>You</span>}
                          </div>
                        )) : <div className={styles.emptyState}>No Google competitors were returned for this point.</div>}
                      </div>
                    </>
                  ) : (
                    <div className={styles.emptyState}>Pick a grid cell to inspect the competitor stack.</div>
                  )}
                </Card>

                <Card>
                  <CardHeader
                    title="Competitor leaderboard"
                    subtitle={competitorSummary.length ? 'Most visible businesses across the current scan.' : 'Run a scan to build the leaderboard.'}
                  />
                  {competitorSummary.length ? (
                    <div className={styles.competitorList}>
                      {competitorSummary.map((competitor) => (
                        <div key={competitor.place_id || competitor.name} className={styles.competitorRow}>
                          <div className={styles.competitorRank}>#{competitor.best_rank}</div>
                          <div className={styles.competitorCopy}>
                            <strong>{competitor.name}</strong>
                            <span>{competitor.address || 'Address unavailable'}</span>
                          </div>
                          <div className={styles.competitorMeta}>
                            <span>{competitor.grid_share}/{results.length} cells</span>
                            <span>Avg #{competitor.avg_rank}</span>
                          </div>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <div className={styles.emptyState}>No recurring competitors to show yet.</div>
                  )}
                </Card>
              </div>
            )}

            {(debugInfo || apiTestLines.length > 0) && (
              <Card>
                <CardHeader title="Diagnostics" subtitle="Use this when live Google Maps results look thin or the fallback Places checks are carrying too much of the scan." />
                {apiTestLines.length > 0 && (
                  <div className={styles.debugBlock}>
                    <div className={styles.debugLabel}>API test</div>
                    {apiTestLines.map(line => <p key={line}>{line}</p>)}
                  </div>
                )}
                {debugInfo && (
                  <div className={styles.debugBlock}>
                    <div className={styles.debugLabel}>Last scan</div>
                    <p>Matched listing: {debugInfo.business_name_used || 'Unknown'}</p>
                    <p>Place ID: {debugInfo.place_id_used || 'Not linked'}</p>
                    <p>Rank provider: {debugInfo.rank_provider || 'Unknown'}</p>
                    <p>Results returned at sample point: {debugInfo.places_returned ?? 0}</p>
                    {(debugInfo.place_id_verification?.address || '').toLowerCase().includes('service-area business') && (
                      <p>This linked listing is a service-area business. It may rank in standard Google results while still appearing inconsistently in Google Maps and local-pack results.</p>
                    )}
                    {debugInfo.keyword_warning && <p>{debugInfo.keyword_warning}</p>}
                  </div>
                )}
              </Card>
            )}
          </div>

          <div className={styles.sidebarColumn}>
            <Card>
              <CardHeader title="Scan controls" subtitle="Your business location stays fixed. The scan center changes per run." />
              <div className={styles.sideMetaList}>
                <div><span>Business location</span><strong>{formatCoords(brand?.business_lat, brand?.business_lng)}</strong></div>
                <div><span>Live center</span><strong>{formatCoords(center?.lat, center?.lng)}</strong></div>
                <div><span>Distance from business</span><strong>{distanceFromBusiness < 0.1 ? '< 0.1 mi' : `${distanceFromBusiness.toFixed(1)} mi`}</strong></div>
                <div><span>Place ID</span><strong>{brand?.google_place_id ? 'Linked' : 'Not linked'}</strong></div>
              </div>

              <button className={styles.secondaryWideButton} type="button" onClick={handleTestApi} disabled={apiTestBusy}>
                {apiTestBusy ? <LoaderCircle size={16} className={styles.spinning} /> : <Wrench size={16} />}
                Test API key
              </button>
            </Card>

            <Card>
              <CardHeader title="Place ID" subtitle="Link the correct Google Business listing for tighter ranking matches." />
              <label className={styles.field}>
                <span>Find listing</span>
                <input value={placeSearchQuery} onChange={(event) => setPlaceSearchQuery(event.target.value)} placeholder={brand?.display_name || 'Business name'} />
              </label>
              <button className={styles.secondaryWideButton} type="button" onClick={handlePlaceSearch} disabled={placeSearchBusy}>
                {placeSearchBusy ? <LoaderCircle size={16} className={styles.spinning} /> : <Search size={16} />}
                Search listings
              </button>

              {placeSearchResults.length > 0 && (
                <div className={styles.searchResults}>
                  {placeSearchResults.map(result => (
                    <button key={result.place_id} type="button" className={styles.searchResult} onClick={() => savePlaceId(result.place_id)}>
                      <strong>{result.name}</strong>
                      <span>{result.address}</span>
                    </button>
                  ))}
                </div>
              )}

              <label className={styles.field}>
                <span>Manual Place ID</span>
                <input value={manualPlaceId} onChange={(event) => setManualPlaceId(event.target.value)} placeholder="ChIJ..." />
              </label>
              <button className={styles.secondaryWideButton} type="button" onClick={() => savePlaceId(manualPlaceId)}>
                <CheckCircle2 size={16} />
                Save Place ID
              </button>
              {placeMessage && <p className={styles.inlineMessage}>{placeMessage}</p>}
            </Card>

            <Card>
              <CardHeader
                title="Scan history"
                subtitle={history.length ? `${history.length} recent scans` : 'Run the first scan to create history.'}
                action={history.length ? (
                  <button type="button" className={styles.clearButton} onClick={handleClearScans}>Clear all</button>
                ) : null}
              />

              {history.length ? (
                <div className={styles.historyList}>
                  {history.map(scan => (
                    <div key={scan.id} className={`${styles.historyItem} ${activeScanId === scan.id ? styles.historyItemActive : ''}`}>
                      <button type="button" className={styles.historyMain} onClick={() => handleLoadScan(scan.id)}>
                        <span className={styles.historyTitle}>{scan.keyword}</span>
                        <span className={styles.historyMeta}>{scan.radius_miles} mi | {scan.grid_size} x {scan.grid_size}</span>
                        <span className={styles.historyMeta}>{scan.avg_rank > 0 ? `Avg local #${scan.avg_rank}` : 'Not returned in scan'}</span>
                        <span className={styles.historyMeta}>{scan.scanned_at?.slice(0, 16) || 'Just now'}</span>
                      </button>
                      <button type="button" className={styles.historyDelete} onClick={() => handleDeleteScan(scan.id)}>
                        <Trash2 size={14} />
                      </button>
                    </div>
                  ))}
                </div>
              ) : (
                <div className={styles.emptySidebar}><History size={16} /> No saved scans yet.</div>
              )}
            </Card>

            {hasLocation && (
              <Card>
                <CardHeader title="Update business location" subtitle="Use autocomplete here too if the listing anchor itself needs to move." />
                <label className={styles.field}>
                  <span>Business address</span>
                  <input
                    ref={locationInputRef}
                    value={locationInput}
                    onChange={(event) => setLocationInput(event.target.value)}
                    placeholder="123 Main St, Phoenix, AZ 85001"
                  />
                </label>
                <button className={styles.secondaryWideButton} type="button" onClick={handleSaveBusinessLocation} disabled={saveLocationBusy}>
                  {saveLocationBusy ? <LoaderCircle size={16} className={styles.spinning} /> : <MapPin size={16} />}
                  Save business location
                </button>
                {locationMessage && <p className={styles.inlineMessage}>{locationMessage}</p>}
              </Card>
            )}
          </div>
        </div>
      )}
    </MotionDiv>
  )
}
