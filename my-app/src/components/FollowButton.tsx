type FollowButtonProps = {
  isFollowing: boolean;
  onToggle: () => void;
};

export default function FollowButton({ isFollowing, onToggle }: FollowButtonProps) {
  return (
    <button
      type="button"
      className={`follow-btn ${isFollowing ? "active" : ""}`}
      onClick={onToggle}
      aria-pressed={isFollowing}
    >
      <span className="follow-icon">
        {isFollowing ? (
          <svg viewBox="0 0 24 24" fill="currentColor" aria-hidden="true">
            <path d="M12 17.3l6.18 3.73-1.64-7.03L22 9.24l-7.19-.61L12 2 9.19 8.63 2 9.24l5.46 4.76-1.64 7.03z" />
          </svg>
        ) : (
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" aria-hidden="true">
            <path d="M12 17.3l6.18 3.73-1.64-7.03L22 9.24l-7.19-.61L12 2 9.19 8.63 2 9.24l5.46 4.76-1.64 7.03z" />
          </svg>
        )}
      </span>

      <span>{isFollowing ? "Following" : "Follow"}</span>
    </button>
  );
}
