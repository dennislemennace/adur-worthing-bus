# Online Safety Act 2023: scope and risk assessments

Worthing Brighton Bus (worthingbrightonbus.co.uk). Assessed 15 September 2026
by Connor R, who runs the site and is the person accountable for the duties
below. This is the written record the Act and Ofcom's guidance expect. It is a
volunteer's good-faith assessment, not legal advice; review it against Ofcom's
current guidance and its risk assessment toolkit.

**Review by 15 September 2027, or sooner if the site changes in a way listed
under "What would change this" at the end.**

---

## 1. Is the site in scope?

The site has links with the United Kingdom (it is aimed at people in West
Sussex and Brighton), so the question is whether it is a regulated
user-to-user service or search service under Part 3.

### User-to-user service: treated as in scope

Section 3 defines a user-to-user service as one "by means of which content that
is generated directly on the service by a user of the service, or uploaded to
or shared on the service by a user of the service, may be encountered by
another user". It does not matter how much content that is, or whether any is
actually shared.

Readers can send in ideas, news, stop and bus reports, and route proposals
through forms on the site. Nothing is public when sent: submissions go to a
private inbox and a person decides what is published. Once approved, a
submission can appear on the site with the name the sender gave, and other
readers can see it.

The project's reasoning:

- An argument exists that this is not user-to-user: no reader publishes
  anything themselves, and what appears is chosen and edited by the provider,
  like a letter printed by a newspaper.
- Against that, section 55(7) says user-generated content is not to be regarded
  as provider content merely because the provider publishes it, pre-moderation
  is not one of the exemptions in Schedule 1, and the one nearby exemption
  (comments and reviews on provider content, Schedule 1 paragraph 4) does not
  fit standalone ideas and reports.
- **Decision: the site is treated as a small, low-risk user-to-user service**,
  and the duties below are met. That is the cautious reading and the duties are
  proportionate for a service like this.

The "Report an error" link opens this project's issue tracker on GitHub. That
tracker is part of GitHub's service, and GitHub is the provider responsible for
it, so it is outside this assessment.

### Search service: considered out of scope, assessed as a precaution

Section 229 defines a search engine as a service or functionality that enables
a person to search "some websites or databases", and says it "does not include
a service which enables a person to search just one website or database".
Ofcom's guidance adds that this includes vertical search engines, focused on a
topic and returning results through an underlying API.

What the site's search-like features do:

| Feature | What it looks through |
|---|---|
| Stop and station search | The site's own stop list (`data/stops.json`), built from NaPTAN |
| Journey checker | The site's own timetable database, built weekly from BODS |
| A stop's departure board | The site's own timetable, with live predictions for that one stop added from TransportAPI |
| A station's train board | Realtime Trains, for that one station |
| Councillor lookup | One postcode in postcodes.io, to find a ward |

- Readers never search other websites, and no result is a link to, or content
  from, a third-party website. The site shows travel times it has processed.
- The stop search and journey checker search one database, the project's own,
  which the section 229 exclusion covers.
- The live boards combine data from more than one source for a stop the reader
  has already chosen. The project's view is that choosing a stop and seeing its
  times is not searching those databases, but a reader of Ofcom's "vertical
  search" wording could disagree.
- **Decision: not treated as a search service**, but section 4 below assesses
  the search-like features anyway, so the record is complete if that view is
  wrong.

---

## 2. Illegal content risk assessment (user-to-user)

### How the service works, as it affects risk

| Factor | This site |
|---|---|
| What users can post | Short text (idea, news item, report), an optional name, an optional source link (news), and a drawn route with stop names (proposals) |
| What users cannot do | Upload images, video or files; message other users; follow, friend or reply to anyone; comment; livestream; create accounts or profiles |
| When content becomes visible | Only after a person has read and approved it. Every submission is pre-moderated. |
| Volume | Low: a handful of submissions a week at most |
| Who uses it | Members of the public in the area, including some teenagers who use the buses |
| Anonymity | Names are optional; no account is needed |
| Recommendation or ranking of user content | None |
| Anti-abuse | Cloudflare Turnstile, per-client rate limits, a global daily cap, server-side text sanitising |

### Assessment of each kind of priority illegal harm

Likelihood reflects whether illegal content of that kind could reach other
users through this service. Because nothing is published without review, the
main residual risk is a moderator failing to recognise illegal content.

| Kind of illegal harm | Risk | Reason |
|---|---|---|
| Terrorism | Low | Text only, pre-moderated, no audience-building features |
| Child sexual exploitation and abuse (grooming, image-based, URLs) | Negligible | No messaging or contact between users, no images; links in news submissions are checked before publishing |
| Hate | Low | Possible in free text; removed at review |
| Harassment, stalking, threats and abuse | Low | Most plausible harm (for example abuse of a named driver or councillor); removed at review, and the terms forbid naming members of the public |
| Controlling or coercive behaviour | Negligible | No user-to-user contact |
| Intimate image abuse | Negligible | No image upload |
| Extreme pornography | Negligible | No image upload |
| Sexual exploitation of adults | Negligible | No contact between users, no advertising |
| Human trafficking | Negligible | No contact between users, no advertising |
| Unlawful immigration | Negligible | Off-topic, pre-moderated |
| Fraud and financial offences | Low | Spam or scam links in news submissions; links are checked and nothing is published automatically |
| Proceeds of crime | Negligible | No transactions or advertising |
| Drugs and psychoactive substances | Negligible | No selling or contact features |
| Firearms, knives and other weapons | Negligible | No selling or contact features |
| Encouraging or assisting suicide or serious self-harm | Low | Possible in free text; removed at review |
| Foreign interference | Low | Campaign content about local politics; pre-moderated, low reach |
| Animal cruelty | Negligible | Text only, off-topic |

Other illegal content (for example false or threatening communications) is
covered by the same review. **Overall: low risk.**

### Measures in place

- **Accountability:** Connor R is responsible for compliance with the online
  safety duties and for this record.
- **Content moderation:** every submission is read before anything is
  published. Illegal content is not published; if something illegal is found
  after publication, it is removed as soon as it is found. Rejected submissions
  are deleted within 30 days (`privacy.html`).
- **Reporting and complaints:** anyone can report content they think is illegal
  or harmful, or complain about a decision or about how these duties are met, by
  email to privacy@worthingbrightonbus.co.uk. The route is set out in
  `terms.html` ("Reporting content and complaints"). Reports are acknowledged and
  acted on promptly; content found to be illegal is removed.
- **Terms of service:** `terms.html` says what is not allowed, that everything
  is reviewed before publication, how to report and complain, and what happens
  next. It is linked from every page footer.
- **Design:** private inbox, a publication box on every form, no images, no
  messaging, no accounts, rate limits and bot checks.
- **Records:** keep a note of each report received, the decision and the date
  (a private note or a label on the inbox issue is enough), and keep this
  document's history in git.

---

## 3. Children's access assessment and children's risk assessment

### Access assessment

1. **Is it possible for children to use the service?** Yes. There is no age
   check, and the site is public.
2. **Is the child user condition met?** Likely yes. Nothing about the site is
   aimed at children, but it is a local bus and train tracker, and secondary
   school pupils and college students use these buses daily. The project cannot
   show that only an insignificant number of children use it, so it assumes the
   condition is met.

**Result: likely to be accessed by children, so a children's risk assessment
is carried out.**

### Children's risk assessment

| Content harmful to children | Risk | Reason |
|---|---|---|
| Pornography | Negligible | No images; text reviewed before publishing |
| Suicide, self-harm, eating disorders (primary priority) | Low | Possible in free text; not published |
| Abuse and hate, bullying | Low | Not published; the terms forbid naming members of the public |
| Violent content, dangerous stunts and challenges | Low | Off-topic for the site; not published |
| Harmful substances | Negligible | Off-topic; not published |
| Other content harmful to children | Low | Everything is reviewed with a general audience, including teenagers, in mind |

Children cannot contact other users, and no user content is recommended or
pushed to anyone. Because content harmful to children is not allowed and
nothing is published without review, the site does not use age assurance.
**Overall: low risk.** The measures in section 2 apply, and moderation
decisions consider whether content is suitable for a general audience that
includes children.

---

## 4. Search-like features: precautionary assessment

If a regulator took the view that the live boards are a vertical search
service, the relevant risk is illegal or harmful content appearing in, or being
reached through, results.

- Results are stop names, service numbers, destinations, and scheduled or
  predicted times, drawn from government and operator transport data
  (BODS, NaPTAN, TransportAPI, Realtime Trains). They contain no user content
  and no links to third-party websites.
- Stop names and destinations come from official datasets and are shown as
  text, escaped before display.
- No search suggestions or predictive search are generated from what other
  people have searched for.

**Risk of illegal content or content harmful to children in results:
negligible.** If a data feed ever contained something inappropriate (for
example a vandalised stop name in an open dataset), it would be reported by
email as above and removed or overridden in the site's data.

---

## 5. What would change this

Review this document straight away if the site:

- lets people publish without review, comment, reply, react, or message each other,
- accepts images, video or files,
- adds accounts, profiles or any way to find or follow other users,
- adds a search across other websites or across more third-party databases,
- starts recommending or ranking user content,
- sees a real increase in submissions, or any report of illegal content, or
- is told by Ofcom that its view of scope is wrong.

## Sources

- Online Safety Act 2023: [section 3](https://www.legislation.gov.uk/ukpga/2023/50/section/3),
  [section 55](https://www.legislation.gov.uk/ukpga/2023/50/section/55),
  [section 229](https://www.legislation.gov.uk/ukpga/2023/50/section/229),
  [Schedule 1](https://www.legislation.gov.uk/ukpga/2023/50/schedule/1)
- Ofcom: [guide for services](https://www.ofcom.org.uk/online-safety/illegal-and-harmful-content/guide-for-services),
  [regulatory documents and guidance](https://www.ofcom.org.uk/online-safety/illegal-and-harmful-content/online-safety-regulatory-documents)
