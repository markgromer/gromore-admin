import { Routes, Route, Navigate } from 'react-router-dom'
import { useEffect } from 'react'
import { useAuthStore } from './stores/authStore'
import Shell from './components/layout/Shell'
import Dashboard from './pages/dashboard/Dashboard'
import Login from './pages/login/Login'
import PlaceholderPage from './pages/PlaceholderPage'
import LegacyRedirectPage from './pages/LegacyRedirectPage'
import { ShimmerPage } from './components/ui/Shimmer'

function ProtectedRoute({ children }) {
  const { user, loading } = useAuthStore()
  if (loading) return <ShimmerPage />
  if (!user) return <Navigate to="/login" replace />
  return children
}

export default function App() {
  const init = useAuthStore(s => s.init)

  useEffect(() => {
    init()
  }, [init])

  return (
    <Routes>
      <Route path="/login" element={<Login />} />

      <Route
        path="/"
        element={
          <ProtectedRoute>
            <Shell />
          </ProtectedRoute>
        }
      >
        <Route index element={<Dashboard />} />
        <Route path="campaigns" element={<PlaceholderPage title="Campaigns" emoji="📢" />} />
        <Route path="crm" element={<PlaceholderPage title="CRM" emoji="👥" />} />
        <Route path="actions" element={<PlaceholderPage title="Missions" emoji="📋" />} />
        <Route path="hiring" element={<PlaceholderPage title="Hiring Hub" />} />
        <Route path="ad-builder" element={<PlaceholderPage title="Ad Builder" />} />
        <Route
          path="creative"
          element={
            <LegacyRedirectPage
              title="Creative Center"
              to="/client/creative"
              description="Redirecting to the full Creative Center."
            />
          }
        />
        <Route path="blog" element={<PlaceholderPage title="Blog" />} />
        <Route path="post-scheduler" element={<PlaceholderPage title="Post Scheduler" />} />
        <Route path="quick-launch" element={<PlaceholderPage title="Quick Launch" />} />
        <Route path="coaching" element={<PlaceholderPage title="Coaching" />} />
        <Route path="my-business" element={<PlaceholderPage title="My Business" />} />
        <Route path="google-business-profile" element={<PlaceholderPage title="Google Profile" />} />
        <Route path="competitors" element={<PlaceholderPage title="Competitor Intel" />} />
        <Route path="kpis" element={<PlaceholderPage title="KPIs" />} />
        <Route path="heatmap" element={<PlaceholderPage title="Rank Heatmap" />} />
        <Route path="settings" element={<PlaceholderPage title="Connections" />} />
        <Route path="feedback" element={<PlaceholderPage title="Feedback" />} />
        <Route path="help" element={<PlaceholderPage title="Help" />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Route>
    </Routes>
  )
}
