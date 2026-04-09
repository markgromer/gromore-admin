import { useEffect } from 'react'
import { motion } from 'framer-motion'
import { useDashboardStore } from '../../stores/dashboardStore'
import { useAuthStore } from '../../stores/authStore'
import { ShimmerPage } from '../../components/ui/Shimmer'
import Card, { CardHeader } from '../../components/ui/Card'
import {
  AlertTriangle, CheckCircle2, Megaphone,
  RefreshCw, Zap
} from 'lucide-react'
import styles from './Dashboard.module.css'

const MotionDiv = motion.div

const stagger = {
  animate: { transition: { staggerChildren: 0.05 } },
}

export default function Dashboard() {
  const { data, loading, error, fetch: fetchDashboard, month } = useDashboardStore()
  const { brand } = useAuthStore()

  useEffect(() => {
    fetchDashboard()
  }, [fetchDashboard, month])

  if (loading && !data) return <ShimmerPage />

  const campaigns = data?.campaigns || {}
  const briefing = data?.warren_briefing
  const healthSummary = data?.health_summary
  const team = data?.team_status
  const googleCount = (campaigns.google || []).length
  const metaCount = (campaigns.meta || []).length

  return (
    <MotionDiv variants={stagger} initial="initial" animate="animate">
      {/* Header */}
      <div className={styles.header}>
        <div>
          <h1 className={styles.title}>
            Welcome back{brand?.display_name ? `, ${brand.display_name}` : ''}
          </h1>
          <p className={styles.subtitle}>Here's how your marketing is performing this month.</p>
        </div>
        <button
          className={styles.refreshBtn}
          onClick={() => fetchDashboard({ refresh: true })}
          disabled={loading}
        >
          <RefreshCw size={16} className={loading ? styles.spinning : ''} />
          Refresh
        </button>
      </div>

      {error && (
        <div className={styles.error}>
          <AlertTriangle size={16} />
          {error}
        </div>
      )}

      {healthSummary && (
        <Card className={styles.healthCard}>
          <div className={styles.healthHeader}>
            <div>
              <div className={`${styles.healthTone} ${styles[`tone${healthSummary.tone?.replace('_', '') || 'neutral'}`]}`}>
                {healthSummary.label}
              </div>
              <h2 className={styles.healthTitle}>Health Meter</h2>
              <p className={styles.healthText}>{healthSummary.summary}</p>
            </div>
            <div className={styles.healthGradeWrap}>
              <div className={styles.healthGrade}>{healthSummary.grade || 'N/A'}</div>
              <div className={styles.healthGradeLabel}>{healthSummary.grade_label || 'Health score'}</div>
            </div>
          </div>

          <div className={styles.healthMeterTrack}>
            <div
              className={`${styles.healthMeterFill} ${styles[`tone${healthSummary.tone?.replace('_', '') || 'neutral'}`]}`}
              style={{ width: `${healthSummary.meter_pct || 0}%` }}
            />
          </div>

          {healthSummary.numbers?.length > 0 && (
            <div className={styles.healthNumbers}>
              {healthSummary.numbers.map((item) => (
                <span key={item} className={styles.healthNumberPill}>{item}</span>
              ))}
            </div>
          )}

          {healthSummary.actions?.length > 0 && (
            <div className={styles.healthActions}>
              <div className={styles.healthActionsLabel}>What to do</div>
              <ul className={styles.healthActionsList}>
                {healthSummary.actions.map((action) => (
                  <li key={action}>{action}</li>
                ))}
              </ul>
            </div>
          )}
        </Card>
      )}

      {/* Briefing + Campaigns Row */}
      <div className={styles.twoCol}>
        {/* Warren Briefing */}
        {briefing && (
          <Card>
            <CardHeader title="Intelligence Briefing" subtitle="Key findings from your data" />
            <div className={styles.findings}>
              {briefing.top_critical?.map((f, i) => (
                <div key={i} className={`${styles.finding} ${styles.critical}`}>
                  <AlertTriangle size={14} />
                  <div>
                    <strong>{f.title}</strong>
                    <p>{f.detail}</p>
                  </div>
                </div>
              ))}
              {briefing.top_wins?.map((f, i) => (
                <div key={i} className={`${styles.finding} ${styles.positive}`}>
                  <CheckCircle2 size={14} />
                  <div>
                    <strong>{f.title}</strong>
                    <p>{f.detail}</p>
                  </div>
                </div>
              ))}
            </div>
          </Card>
        )}

        {/* Campaign Summary */}
        <Card>
          <CardHeader
            title="Active Campaigns"
            subtitle={`${googleCount} Google, ${metaCount} Meta`}
          />
          <div className={styles.campSummary}>
            {googleCount > 0 && (
              <div className={styles.campPlatform}>
                <Megaphone size={16} />
                <span>Google Ads</span>
                <span className={styles.campCount}>{googleCount} campaigns</span>
              </div>
            )}
            {metaCount > 0 && (
              <div className={styles.campPlatform}>
                <Zap size={16} />
                <span>Meta Ads</span>
                <span className={styles.campCount}>{metaCount} campaigns</span>
              </div>
            )}
            {googleCount === 0 && metaCount === 0 && (
              <p className={styles.empty}>No active campaigns yet. Create your first one to get started.</p>
            )}
          </div>
        </Card>
      </div>

      {/* Team Status */}
      {team && (
        <Card>
          <CardHeader title="Your Team" subtitle={`${team.trained || 0} agents active`} />
          <div className={styles.teamBar}>
            <div
              className={styles.teamFill}
              style={{ width: `${((team.trained || 0) / (team.total_available || 1)) * 100}%` }}
            />
          </div>
          <p className={styles.teamLabel}>
            {team.trained || 0} of {team.total_available || 0} agents trained and deployed
          </p>
        </Card>
      )}

      {data?._cached && (
        <p className={styles.cacheNote}>
          Showing cached data from {new Date(data._cached_at).toLocaleString()}.
          <button onClick={() => fetchDashboard({ refresh: true })} className={styles.refreshLink}>
            Refresh now
          </button>
        </p>
      )}
    </MotionDiv>
  )
}
