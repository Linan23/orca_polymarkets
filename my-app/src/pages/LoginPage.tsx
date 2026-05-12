import { useEffect, useState } from "react";
import { Link, useNavigate, useSearchParams } from "react-router-dom";
import { useAuth } from "../auth/AuthContext";
import TopNavbar from "../homepage/TopNavbar";

type LoginMode = "signin" | "signup" | "reset";

function isPrivilegedRole(role?: string | null) {
  return role === "moderator" || role === "admin";
}

export default function LoginPage() {
  const {
    loading,
    isAuthenticated,
    account,
    login,
    signup,
    verifyEmail,
    verifyMfa,
    requestPasswordReset,
    confirmPasswordReset,
    setupMfa,
    enableMfa,
  } = useAuth();
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const [mode, setMode] = useState<LoginMode>(searchParams.get("reset") ? "reset" : "signin");
  const [displayName, setDisplayName] = useState("");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [resetToken] = useState(searchParams.get("reset") ?? "");
  const [mfaToken, setMfaToken] = useState<string | null>(null);
  const [mfaCode, setMfaCode] = useState("");
  const [mfaSetupSecret, setMfaSetupSecret] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const returnTo = searchParams.get("returnTo") || "/following";
  const verificationToken = searchParams.get("verify");
  const needsMfaSetup = isAuthenticated && isPrivilegedRole(account?.role) && !account?.mfa_enabled;

  useEffect(() => {
    if (!verificationToken) return;
    let cancelled = false;
    setSubmitting(true);
    setError(null);
    verifyEmail(verificationToken)
      .then(() => {
        if (!cancelled) {
          setNotice("Email verified. Your account is ready.");
          navigate(returnTo, { replace: true });
        }
      })
      .catch((verifyError) => {
        if (!cancelled) {
          setError(verifyError instanceof Error ? verifyError.message : "Unable to verify this email link.");
        }
      })
      .finally(() => {
        if (!cancelled) setSubmitting(false);
      });
    return () => {
      cancelled = true;
    };
  }, [navigate, returnTo, verificationToken, verifyEmail]);

  useEffect(() => {
    if (!loading && isAuthenticated && !needsMfaSetup) {
      navigate(returnTo, { replace: true });
    }
  }, [isAuthenticated, loading, navigate, needsMfaSetup, returnTo]);

  function switchMode(nextMode: LoginMode) {
    setMode(nextMode);
    setError(null);
    setNotice(null);
    setMfaToken(null);
    setMfaCode("");
  }

  async function handleMfaSubmit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!mfaToken) return;
    setSubmitting(true);
    setError(null);
    try {
      await verifyMfa(mfaToken, mfaCode);
      navigate(returnTo, { replace: true });
    } catch (submitError) {
      setError(submitError instanceof Error ? submitError.message : "Unable to verify MFA code.");
    } finally {
      setSubmitting(false);
    }
  }

  async function handleMfaSetup() {
    setSubmitting(true);
    setError(null);
    try {
      const setup = await setupMfa();
      setMfaSetupSecret(setup.secret);
      setNotice("Add this setup key to your authenticator app, then enter the 6-digit code.");
    } catch (setupError) {
      setError(setupError instanceof Error ? setupError.message : "Unable to start MFA setup.");
    } finally {
      setSubmitting(false);
    }
  }

  async function handleEnableMfa(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setSubmitting(true);
    setError(null);
    try {
      await enableMfa(mfaCode);
      setNotice("MFA is enabled.");
      navigate(returnTo, { replace: true });
    } catch (setupError) {
      setError(setupError instanceof Error ? setupError.message : "Unable to enable MFA.");
    } finally {
      setSubmitting(false);
    }
  }

  async function handleSubmit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setError(null);
    setNotice(null);

    if (mode === "signup" && password !== confirmPassword) {
      setError("Passwords do not match.");
      return;
    }
    if (mode === "reset" && resetToken && password !== confirmPassword) {
      setError("Passwords do not match.");
      return;
    }

    setSubmitting(true);
    try {
      if (mode === "signin") {
        const response = await login({ email, password });
        if (response.mfa_required && response.mfa_token) {
          setMfaToken(response.mfa_token);
          setNotice("Enter the 6-digit code from your authenticator app.");
          return;
        }
        navigate(returnTo, { replace: true });
      } else if (mode === "signup") {
        const response = await signup({
          display_name: displayName,
          email,
          password,
        });
        setNotice(response.message || "Check your email to verify your account.");
        setPassword("");
        setConfirmPassword("");
      } else if (resetToken) {
        await confirmPasswordReset(resetToken, password);
        navigate(returnTo, { replace: true });
      } else {
        await requestPasswordReset(email);
        setNotice("If this account exists, a password reset link has been sent.");
      }
    } catch (submitError) {
      setError(submitError instanceof Error ? submitError.message : "Unable to complete this request right now.");
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <>
      <TopNavbar />

      <div className="login-page">
        <div className="login-card account-login-card">
          <p className="leaderboard-kicker">Account Access</p>
          <h1>
            {needsMfaSetup
              ? "Secure Your Account"
              : mfaToken
                ? "Verify MFA"
                : mode === "signin"
                  ? "Sign In"
                  : mode === "signup"
                    ? "Create Account"
                    : "Reset Password"}
          </h1>
          <p className="login-subtext">
            Public research pages stay open. Sign in is required for following lists and saved preferences.
          </p>

          {needsMfaSetup ? (
            <div className="login-form">
              <p className="login-info">
                Moderator and admin accounts require an authenticator app before protected tools are available.
              </p>
              {!mfaSetupSecret ? (
                <button type="button" onClick={handleMfaSetup} disabled={submitting || loading}>
                  {submitting ? "Preparing..." : "Start MFA Setup"}
                </button>
              ) : (
                <form className="login-form" onSubmit={handleEnableMfa}>
                  <div className="login-token-box">
                    <span>Authenticator setup key</span>
                    <strong>{mfaSetupSecret}</strong>
                  </div>
                  <input
                    value={mfaCode}
                    onChange={(event) => setMfaCode(event.target.value)}
                    type="text"
                    inputMode="numeric"
                    placeholder="6-digit code"
                    autoComplete="one-time-code"
                    required
                  />
                  <button type="submit" disabled={submitting || loading}>
                    {submitting ? "Verifying..." : "Enable MFA"}
                  </button>
                </form>
              )}
            </div>
          ) : mfaToken ? (
            <form className="login-form" onSubmit={handleMfaSubmit}>
              <input
                value={mfaCode}
                onChange={(event) => setMfaCode(event.target.value)}
                type="text"
                inputMode="numeric"
                placeholder="6-digit code"
                autoComplete="one-time-code"
                required
              />
              <button type="submit" disabled={submitting || loading}>
                {submitting ? "Verifying..." : "Verify and Continue"}
              </button>
            </form>
          ) : (
            <>
              <div className="login-mode-toggle" role="tablist" aria-label="Account access mode">
                <button
                  type="button"
                  className={`login-mode-btn ${mode === "signin" ? "active" : ""}`}
                  onClick={() => switchMode("signin")}
                >
                  Sign In
                </button>
                <button
                  type="button"
                  className={`login-mode-btn ${mode === "signup" ? "active" : ""}`}
                  onClick={() => switchMode("signup")}
                >
                  Create Account
                </button>
                <button
                  type="button"
                  className={`login-mode-btn ${mode === "reset" ? "active" : ""}`}
                  onClick={() => switchMode("reset")}
                >
                  Reset Password
                </button>
              </div>

              <form className="login-form" onSubmit={handleSubmit}>
                {mode === "signup" && (
                  <input
                    value={displayName}
                    onChange={(event) => setDisplayName(event.target.value)}
                    type="text"
                    placeholder="Display name"
                    autoComplete="name"
                    required
                  />
                )}
                {(!resetToken || mode !== "reset") && (
                  <input
                    value={email}
                    onChange={(event) => setEmail(event.target.value)}
                    type="email"
                    placeholder="Email"
                    autoComplete="email"
                    required
                  />
                )}
                {(mode !== "reset" || resetToken) && (
                  <input
                    value={password}
                    onChange={(event) => setPassword(event.target.value)}
                    type="password"
                    placeholder={mode === "reset" ? "New password" : "Password"}
                    autoComplete={mode === "signin" ? "current-password" : "new-password"}
                    required
                  />
                )}
                {mode === "reset" && resetToken && (
                  <input
                    value={confirmPassword}
                    onChange={(event) => setConfirmPassword(event.target.value)}
                    type="password"
                    placeholder="Confirm new password"
                    autoComplete="new-password"
                    required
                  />
                )}
                {mode === "signup" && (
                  <>
                    <input
                      value={confirmPassword}
                      onChange={(event) => setConfirmPassword(event.target.value)}
                      type="password"
                      placeholder="Confirm password"
                      autoComplete="new-password"
                      required
                    />
                    <p className="login-info">Signup is limited to approved email domains and requires email verification.</p>
                  </>
                )}

                {error && <div className="login-error">{error}</div>}
                {notice && <div className="login-notice">{notice}</div>}

                <button type="submit" disabled={submitting || loading}>
                  {submitting
                    ? "Working..."
                    : mode === "signin"
                      ? "Sign In"
                      : mode === "signup"
                        ? "Create Account"
                        : resetToken
                          ? "Reset Password"
                          : "Send Reset Link"}
                </button>
              </form>
            </>
          )}

          {(error || notice) && !mfaToken && !needsMfaSetup ? null : notice && <div className="login-notice">{notice}</div>}
          {error && (mfaToken || needsMfaSetup) && <div className="login-error">{error}</div>}

          <p className="login-subnote">
            Email verification, CSRF-protected sessions, and extra admin MFA keep account data safer.
          </p>

          <Link to="/" className="back-home">
            {"<- Back to homepage"}
          </Link>
        </div>
      </div>
    </>
  );
}
