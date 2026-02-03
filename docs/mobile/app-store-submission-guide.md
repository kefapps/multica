# App Store Submission Guide

Complete guide for publishing the Expo React Native app to Apple App Store and Google Play Store.

## 1. Prerequisites

### Accounts & Fees

| Platform | Cost | Notes |
|----------|------|-------|
| Apple Developer Program | $99/year | Required for App Store distribution |
| Google Play Console | $25 one-time | Developer registration |
| Expo Account | Free (paid plans available) | Required for EAS Build/Submit |

- Apple Developer account review: 1-2 days
- Google Play developer account review: days to weeks

### Tools

```bash
npm install -g eas-cli
eas login
eas whoami  # verify login
```

## 2. Project Configuration

### Initialize EAS

```bash
eas build:configure
```

Generates `eas.json` with three build profiles: `development`, `preview`, `production`.

### Key `app.json` / `app.config.ts` Fields

```jsonc
{
  "name": "Multica",
  "slug": "multica",
  "version": "1.0.0",
  "ios": {
    "bundleIdentifier": "com.multica.app",
    "buildNumber": "1"          // increment on each submission
  },
  "android": {
    "package": "com.multica.app",
    "versionCode": 1            // increment on each submission
  },
  "icon": "./assets/icon.png",  // 1024x1024 PNG
  "splash": {
    "image": "./assets/splash.png"
  }
}
```

## 3. App Signing & Credentials

### iOS

- EAS auto-manages credentials (recommended): Distribution Certificate + Provisioning Profile
- Or create manually in Apple Developer Portal

### Android

- EAS auto-generates Keystore (recommended), stored securely on EAS servers
- **Back up Keystore** — losing it means you cannot update the published app
- Play Store requires AAB (Android App Bundle) format

## 4. Production Build

```bash
# iOS
eas build --platform ios --profile production

# Android
eas build --platform android --profile production

# Both
eas build --platform all --profile production
```

Builds run in Expo cloud — no local Xcode or Android Studio needed.

## 5. Store Listing Preparation

### Required for Both Platforms

#### Privacy Policy

- **Mandatory** — must be a publicly accessible URL
- Must clearly state:
  - What data the app collects and how
  - Whether data is shared with third parties
  - Data retention and deletion policies
  - How users can request data deletion
- **2025 rule**: If data is sent to third-party AI, must disclose explicitly and obtain user consent
- Tools: Termly, PrivacyPolicies.com, or custom page

#### App Screenshots

- **iOS**: Multiple sizes required (6.7", 6.5", 5.5" iPhone + iPad)
- **Android**: 2-8 screenshots
- Must accurately reflect current app interface

#### App Icon

- 1024x1024 high-resolution PNG
- No alpha/transparency for iOS

#### App Description

- Short description (≤80 chars for Google Play)
- Full description

#### Support URL

- A link where users can get help

#### Account Deletion

- If the app supports registration, users **must** be able to delete their account and data in-app
- Both Apple and Google require this

### Apple App Store Connect — Additional Requirements

| Item | Details |
|------|---------|
| Privacy Nutrition Labels | Fill out data collection practices per category in App Store Connect |
| App Review Information | Reviewer contact info, demo/test account credentials |
| Content Rating | Age classification |
| Export Compliance | Encryption usage declaration |
| Info.plist Permission Strings | Clear purpose description for each permission (camera, location, etc.) |

### Google Play Console — Additional Requirements

| Item | Details |
|------|---------|
| Data Safety Form | Detail data collection and sharing (required even if no data is collected) |
| Content Rating Questionnaire | IARC rating questionnaire |
| Target Audience | Declare if the app targets children |
| First Upload | Must be done manually via Play Console (Google Play API limitation) |

## 6. Submit to Stores

### Apple App Store

```bash
eas submit --platform ios
```

This uploads the build to **App Store Connect / TestFlight**. Then you must:

1. Log into App Store Connect
2. Select the uploaded build
3. Associate it with a version
4. Fill in all metadata, screenshots, privacy labels
5. Submit for App Review

### Google Play Store

```bash
eas submit --platform android
```

**First time**: Must upload AAB manually in Play Console.

After initial upload:
1. Navigate to Production → Create new release
2. Upload AAB or use the EAS-submitted build
3. Fill in description, screenshots, data safety form
4. Submit for review

### Auto-Submit (Optional)

```bash
eas build --platform all --profile production --auto-submit
```

## 7. App Review

| | Apple | Google |
|---|---|---|
| Review time | Typically 24-48 hours | Hours to 7 days |
| Common rejections | Incomplete features, misleading screenshots, missing privacy policy, unclear permission strings | Data safety form mismatch, policy violations |
| After rejection | Fix issues, resubmit | Fix issues, resubmit |

## 8. Post-Launch

### OTA Updates (No Re-Review Needed)

```bash
eas update --branch production
```

- Only for JS/asset-level changes
- Native code changes still require a new build + review

### CI/CD Automation

Create `.eas/workflows/build-and-submit.yml` to auto-build and submit on push to main.

### Google Service Account Key (for Automated Android Submissions)

1. Go to EAS dashboard → Credentials → Android
2. Click Application identifier → Service Credentials
3. Add Google Service Account Key

## 9. Checklist

- [ ] Register Apple Developer + Google Play Console accounts
- [ ] Configure `app.json` and `eas.json`
- [ ] Prepare app icon, splash screen, screenshots
- [ ] Write and host privacy policy URL
- [ ] Implement in-app account deletion (if registration exists)
- [ ] Add Info.plist permission descriptions (iOS)
- [ ] Run `eas build --platform all --profile production`
- [ ] Create app in App Store Connect, fill metadata + privacy labels
- [ ] Create app in Google Play Console, fill data safety form, manual first AAB upload
- [ ] `eas submit` or submit manually for review
- [ ] Wait for review approval → live
- [ ] Set up `eas update` for OTA updates

## References

- [Expo: Submit to App Stores](https://docs.expo.dev/deploy/submit-to-app-stores/)
- [Expo: EAS Submit](https://docs.expo.dev/submit/introduction/)
- [Expo: Build Your Project](https://docs.expo.dev/deploy/build-project/)
- [Expo: App Stores Best Practices](https://docs.expo.dev/distribution/app-stores/)
- [Apple App Review Guidelines](https://developer.apple.com/app-store/review/guidelines/)
- [Apple App Privacy Details](https://developer.apple.com/app-store/app-privacy-details/)
- [Google Play Data Safety](https://support.google.com/googleplay/android-developer/answer/10787469)
- [Google Play Developer Policy Center](https://play.google/developer-content-policy/)
