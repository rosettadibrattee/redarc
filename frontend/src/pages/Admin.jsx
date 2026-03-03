import { useState } from 'react';
import { watchSubreddit, unlistSubreddit, fetchProgress, adminDeleteByFilter, fetchSubreddits } from '../utils/api';
import { useApi } from '../hooks/useApi';
import { Loading, Badge, Toast } from '../components/UI';
import { toUnixTimestamp } from '../utils/format';

export default function AdminPage() {
  const [toast, setToast] = useState(null);

  // Watch subreddits
  const [watchSub, setWatchSub] = useState('');
  const [watchPw, setWatchPw] = useState('');

  // Unlist
  const [unlistSub, setUnlistSub] = useState('');
  const [unlistPw, setUnlistPw] = useState('');

  // Progress
  const [progressPw, setProgressPw] = useState('');
  const { data: progress, loading: loadingProgress, refetch: refetchProgress } = useApi(
    () => fetchProgress(progressPw),
    [progressPw]
  );
  const { data: dangerSubreddits } = useApi((s) => fetchSubreddits(s), []);

  // Danger zone (delete by filters)
  const [dangerPw, setDangerPw] = useState('');
  const [dangerTarget, setDangerTarget] = useState('submissions');
  const [dangerSub, setDangerSub] = useState('');
  const [dangerAuthor, setDangerAuthor] = useState('');
  const [dangerKeywords, setDangerKeywords] = useState('');
  const [dangerAfter, setDangerAfter] = useState('');
  const [dangerBefore, setDangerBefore] = useState('');
  const [dangerConfirm, setDangerConfirm] = useState('');
  const [dangerPreview, setDangerPreview] = useState(null);
  const [dangerPreviewKey, setDangerPreviewKey] = useState('');
  const [dangerLoading, setDangerLoading] = useState(false);
  const [dangerDeleting, setDangerDeleting] = useState(false);

  const showToast = (type, message) => {
    setToast({ type, message });
    setTimeout(() => setToast(null), 4000);
  };

  const handleWatch = async (action) => {
    if (!watchSub.trim()) return;
    try {
      await watchSubreddit(watchSub.trim().toLowerCase(), action, watchPw);
      showToast('success', `${action === 'add' ? 'Watching' : 'Unwatched'} r/${watchSub}`);
      setWatchSub('');
    } catch (err) {
      showToast('error', err.message);
    }
  };

  const handleUnlist = async (unlist) => {
    if (!unlistSub.trim()) return;
    try {
      await unlistSubreddit(unlistSub.trim().toLowerCase(), unlist, unlistPw);
      showToast('success', `${unlist ? 'Unlisted' : 'Relisted'} r/${unlistSub}`);
      setUnlistSub('');
    } catch (err) {
      showToast('error', err.message);
    }
  };

  const buildDangerPayload = (dryRun) => {
    const normalizedSub = dangerSub.trim().replace(/^r\//i, '').toLowerCase();

    return {
      password: dryRun ? undefined : dangerPw,
      dry_run: dryRun,
      confirm_text: dryRun ? '' : dangerConfirm,
      target: dangerTarget,
      subreddit: normalizedSub || undefined,
      author: dangerAuthor.trim() || undefined,
      keywords: dangerKeywords.trim() || undefined,
      after: toUnixTimestamp(dangerAfter)?.toString(),
      before: toUnixTimestamp(dangerBefore)?.toString(),
    };
  };

  const getDangerReviewKey = () => JSON.stringify({
    target: dangerTarget,
    subreddit: dangerSub.trim().replace(/^r\//i, '').toLowerCase(),
    author: dangerAuthor.trim().toLowerCase(),
    keywords: dangerKeywords.trim(),
    after: toUnixTimestamp(dangerAfter)?.toString() || '',
    before: toUnixTimestamp(dangerBefore)?.toString() || '',
  });

  const handleDangerDelete = async () => {
    if (!dangerSub.trim()) {
      showToast('error', 'Enter a subreddit');
      return;
    }

    const reviewKey = getDangerReviewKey();
    const reviewReady = dangerPreview && dangerPreviewKey === reviewKey;

    if (!reviewReady) {
      setDangerLoading(true);
      try {
        const data = await adminDeleteByFilter(buildDangerPayload(true));
        setDangerPreview(data);
        setDangerPreviewKey(reviewKey);
        setDangerConfirm('');
        showToast('success', `Review ready: ${data.counts?.main || 0} rows matched`);
      } catch (err) {
        setDangerPreview(null);
        setDangerPreviewKey('');
        showToast('error', err.message);
      } finally {
        setDangerLoading(false);
      }
      return;
    }

    if (dangerConfirm !== 'DELETE') {
      showToast('error', 'Type DELETE to confirm');
      return;
    }
    if (!dangerPw.trim()) {
      showToast('error', 'Enter admin password');
      return;
    }
    setDangerDeleting(true);
    try {
      const data = await adminDeleteByFilter(buildDangerPayload(false));
      showToast('success', `Deleted ${data.deleted?.main || 0} rows from main DB`);
      setDangerPreview(null);
      setDangerPreviewKey('');
      setDangerConfirm('');
      setDangerPw('');
    } catch (err) {
      showToast('error', err.message);
    } finally {
      setDangerDeleting(false);
    }
  };

  return (
    <div className="max-w-[800px] mx-auto">
      <div className="font-serif text-[28px] font-semibold text-text-primary text-center mb-2">
        Admin
      </div>
      <p className="text-center text-[13px] text-text-tertiary mb-6">
        Manage subreddits, watch lists, and ingest jobs
      </p>

      <div className="grid grid-cols-1 sm:grid-cols-2 gap-4 mb-4">
        {/* Watch subreddits */}
        <div className="p-5 bg-bg-secondary border border-border-subtle rounded-xl">
          <h3 className="text-[13px] font-semibold uppercase tracking-wider text-text-secondary mb-2">
            Watch Subreddits
          </h3>
          <p className="text-xs text-text-tertiary mb-3">
            Add subreddits for periodic fetching of new/hot/rising threads
          </p>
          <div className="space-y-2">
            <input
              className="w-full p-2.5 rounded-lg text-[13px] bg-bg-tertiary border border-border text-text-primary outline-none focus:border-accent transition-colors"
              placeholder="subreddit name"
              value={watchSub}
              onChange={(e) => setWatchSub(e.target.value)}
            />
            <input
              type="password"
              className="w-full p-2.5 rounded-lg text-[13px] bg-bg-tertiary border border-border text-text-primary outline-none focus:border-accent transition-colors"
              placeholder="Admin password"
              value={watchPw}
              onChange={(e) => setWatchPw(e.target.value)}
            />
            <div className="flex gap-2">
              <button
                onClick={() => handleWatch('add')}
                className="flex-1 px-3 py-2 rounded-lg text-xs font-semibold uppercase bg-accent text-white hover:bg-accent-hover transition-all"
              >
                Watch
              </button>
              <button
                onClick={() => handleWatch('remove')}
                className="flex-1 px-3 py-2 rounded-lg text-xs font-semibold uppercase bg-bg-tertiary text-text-secondary border border-border hover:bg-bg-hover transition-all"
              >
                Unwatch
              </button>
            </div>
          </div>
        </div>

        {/* Unlist */}
        <div className="p-5 bg-bg-secondary border border-border-subtle rounded-xl">
          <h3 className="text-[13px] font-semibold uppercase tracking-wider text-text-secondary mb-2">
            Unlist Subreddit
          </h3>
          <p className="text-xs text-text-tertiary mb-3">
            Hide subreddits from the public index without deleting data
          </p>
          <div className="space-y-2">
            <input
              className="w-full p-2.5 rounded-lg text-[13px] bg-bg-tertiary border border-border text-text-primary outline-none focus:border-accent transition-colors"
              placeholder="subreddit name"
              value={unlistSub}
              onChange={(e) => setUnlistSub(e.target.value)}
            />
            <input
              type="password"
              className="w-full p-2.5 rounded-lg text-[13px] bg-bg-tertiary border border-border text-text-primary outline-none focus:border-accent transition-colors"
              placeholder="Admin password"
              value={unlistPw}
              onChange={(e) => setUnlistPw(e.target.value)}
            />
            <div className="flex gap-2">
              <button
                onClick={() => handleUnlist(true)}
                className="flex-1 px-3 py-2 rounded-lg text-xs font-semibold uppercase bg-bg-tertiary text-text-secondary border border-border hover:bg-bg-hover transition-all"
              >
                Unlist
              </button>
              <button
                onClick={() => handleUnlist(false)}
                className="flex-1 px-3 py-2 rounded-lg text-xs font-semibold uppercase bg-accent text-white hover:bg-accent-hover transition-all"
              >
                Relist
              </button>
            </div>
          </div>
        </div>
      </div>

      {/* Job history */}
      <div className="p-5 bg-bg-secondary border border-border-subtle rounded-xl">
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-[13px] font-semibold uppercase tracking-wider text-text-secondary">
            Ingest Job History
          </h3>
          <div className="flex gap-2 items-center">
            <input
              type="password"
              className="p-1.5 rounded text-[11px] bg-bg-tertiary border border-border text-text-primary outline-none focus:border-accent w-32"
              placeholder="Admin pw (optional)"
              value={progressPw}
              onChange={(e) => setProgressPw(e.target.value)}
            />
            <button
              onClick={refetchProgress}
              className="px-3 py-1.5 rounded text-[11px] font-semibold uppercase bg-bg-tertiary text-text-secondary border border-border hover:bg-bg-hover transition-all"
            >
              Refresh
            </button>
          </div>
        </div>

        {loadingProgress && <Loading />}

        {!loadingProgress && progress && (
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-border">
                  <th className="text-left p-2 text-text-tertiary font-medium">Job ID</th>
                  <th className="text-left p-2 text-text-tertiary font-medium">URL</th>
                  <th className="text-left p-2 text-text-tertiary font-medium">Started</th>
                  <th className="text-left p-2 text-text-tertiary font-medium">Finished</th>
                  <th className="text-left p-2 text-text-tertiary font-medium">Status</th>
                </tr>
              </thead>
              <tbody>
                {progress.length === 0 ? (
                  <tr>
                    <td colSpan={5} className="p-6 text-center text-text-tertiary">No jobs found</td>
                  </tr>
                ) : (
                  progress.map((p) => (
                    <tr key={p.job_id} className="border-b border-border-subtle">
                      <td className="p-2.5 text-text-secondary font-mono">{p.job_id}</td>
                      <td className="p-2.5 text-text-tertiary truncate max-w-[200px]">{p.url || '—'}</td>
                      <td className="p-2.5 text-text-tertiary">
                        {p.start_utc ? new Date(p.start_utc * 1000).toLocaleString() : '—'}
                      </td>
                      <td className="p-2.5 text-text-tertiary">
                        {p.finish_utc ? new Date(p.finish_utc * 1000).toLocaleString() : '—'}
                      </td>
                      <td className="p-2.5">
                        <Badge variant={p.error ? 'error' : 'success'}>
                          {p.error ? 'error' : 'success'}
                        </Badge>
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {/* Danger Zone */}
      <div className="mt-4 p-5 bg-bg-secondary border-2 border-accent rounded-xl">
        <h3 className="text-[13px] font-semibold uppercase tracking-wider text-text-secondary mb-2">
          Danger Zone
        </h3>
        <p className="text-xs text-text-tertiary mb-4">
          Permanently delete submissions or comments by filter. This cannot be undone.
        </p>

        <div className="grid grid-cols-1 sm:grid-cols-2 gap-3 mb-3">
          <div>
            <label className="block text-[11px] font-medium text-text-tertiary uppercase tracking-wider mb-1">Target</label>
            <select
              className="w-full p-2.5 rounded-lg text-[13px] bg-bg-tertiary border border-border text-text-primary outline-none focus:border-accent transition-colors"
              value={dangerTarget}
              onChange={(e) => setDangerTarget(e.target.value)}
            >
              <option value="submissions">Submissions</option>
              <option value="comments">Comments</option>
            </select>
          </div>
          <div>
            <label className="block text-[11px] font-medium text-text-tertiary uppercase tracking-wider mb-1">Subreddit</label>
            <select
              className="w-full p-2.5 rounded-lg text-[13px] bg-bg-tertiary border border-border text-text-primary outline-none focus:border-accent transition-colors"
              value={dangerSub}
              onChange={(e) => setDangerSub(e.target.value)}
            >
              <option value="">Select subreddit</option>
              {dangerSubreddits?.map((s) => (
                <option key={s.name} value={s.name}>r/{s.name}</option>
              ))}
            </select>
          </div>
          <div>
            <label className="block text-[11px] font-medium text-text-tertiary uppercase tracking-wider mb-1">Author</label>
            <input
              className="w-full p-2.5 rounded-lg text-[13px] bg-bg-tertiary border border-border text-text-primary outline-none focus:border-accent transition-colors"
              placeholder="optional"
              value={dangerAuthor}
              onChange={(e) => setDangerAuthor(e.target.value)}
            />
          </div>
          <div>
            <label className="block text-[11px] font-medium text-text-tertiary uppercase tracking-wider mb-1">Keywords</label>
            <input
              className="w-full p-2.5 rounded-lg text-[13px] bg-bg-tertiary border border-border text-text-primary outline-none focus:border-accent transition-colors"
              placeholder="optional"
              value={dangerKeywords}
              onChange={(e) => setDangerKeywords(e.target.value)}
            />
          </div>
          <div>
            <label className="block text-[11px] font-medium text-text-tertiary uppercase tracking-wider mb-1">After Date</label>
            <input
              type="date"
              className="w-full p-2.5 rounded-lg text-[13px] bg-bg-tertiary border border-border text-text-primary outline-none focus:border-accent transition-colors"
              value={dangerAfter}
              onChange={(e) => setDangerAfter(e.target.value)}
            />
          </div>
          <div>
            <label className="block text-[11px] font-medium text-text-tertiary uppercase tracking-wider mb-1">Before Date</label>
            <input
              type="date"
              className="w-full p-2.5 rounded-lg text-[13px] bg-bg-tertiary border border-border text-text-primary outline-none focus:border-accent transition-colors"
              value={dangerBefore}
              onChange={(e) => setDangerBefore(e.target.value)}
            />
          </div>
        </div>

        <p className="text-[11px] text-text-tertiary mb-3">
          Click <span className="font-semibold text-accent">Delete</span> once to review current filters, then enter admin password and type <span className="font-mono">DELETE</span> to execute.
        </p>

        {dangerPreview && (
          <div className="p-3 rounded-lg bg-bg-tertiary border border-border mb-3">
            <div className="text-xs text-text-secondary mb-2">Current filters to be applied:</div>
            <div className="text-[11px] text-text-tertiary font-mono break-all mb-2">
              {JSON.stringify(dangerPreview.filters)}
            </div>
            <div className="text-xs text-text-secondary">
              Matching rows: main={dangerPreview.counts?.main || 0}{' '}
              {dangerPreview.fts_enabled
                ? `fts=${dangerPreview.counts?.fts == null ? 'unknown' : dangerPreview.counts.fts}`
                : '(fts disabled)'}
            </div>
          </div>
        )}

        <div className={`grid grid-cols-1 ${dangerPreview ? 'sm:grid-cols-[1fr_1fr_auto]' : 'sm:grid-cols-[1fr_auto]'} gap-3 items-end`}>
          {dangerPreview ? (
            <>
              <div>
                <label className="block text-[11px] font-medium text-text-tertiary uppercase tracking-wider mb-1">
                  Admin Password
                </label>
                <input
                  type="password"
                  className="w-full p-2.5 rounded-lg text-[13px] bg-bg-tertiary border border-border text-text-primary outline-none focus:border-accent transition-colors"
                  placeholder="required to execute"
                  value={dangerPw}
                  onChange={(e) => setDangerPw(e.target.value)}
                />
              </div>
              <div>
                <label className="block text-[11px] font-medium text-text-tertiary uppercase tracking-wider mb-1">
                  Type DELETE to confirm
                </label>
                <input
                  className="w-full p-2.5 rounded-lg text-[13px] bg-bg-tertiary border border-border text-text-primary outline-none focus:border-accent transition-colors"
                  placeholder="DELETE"
                  value={dangerConfirm}
                  onChange={(e) => setDangerConfirm(e.target.value)}
                />
              </div>
            </>
          ) : <div />}
          <button
            onClick={handleDangerDelete}
            disabled={dangerLoading || dangerDeleting}
            className="px-5 py-2.5 rounded-lg text-xs font-semibold uppercase bg-red-700 text-white hover:bg-red-600 disabled:opacity-50 disabled:cursor-not-allowed transition-all"
          >
            {dangerLoading ? 'Reviewing...' : dangerDeleting ? 'Deleting...' : (dangerPreview ? 'Confirm Delete' : 'Delete')}
          </button>
        </div>
      </div>

      {toast && <Toast type={toast.type} message={toast.message} />}
    </div>
  );
}
