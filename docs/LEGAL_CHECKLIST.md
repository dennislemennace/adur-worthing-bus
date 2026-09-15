# Legal and compliance checklist

What this site has to keep true, what was done on 15 September 2026, and what
still needs a person. This is working guidance for a volunteer project, not
legal advice. When a decision carries real risk, get advice.

## Commitments the published pages make

These are promises in `privacy.html` and `terms.html`. If the practice stops
matching them, change the practice or change the page.

- **Submissions not approved are deleted within 30 days of the decision.**
  They sit as issues in the private `adur-worthing-bus-inbox` repository.
  Close and delete rejected or spam issues; a repository admin can delete an
  issue from its page (⋯ → Delete issue).
- **Requests to `privacy@worthingbrightonbus.co.uk` are answered within one
  month.** A removal request means taking the item out of `data/` and deleting
  the inbox issue.
- **Nothing a sender typed is published without review**, and published names
  are only the ones they gave.
- **Visit counting describes itself accurately.** A new `track()` name is a
  change to the list in `privacy.html` (`tests/test_privacy_notice.py` fails
  until it is).
- **Every outside host a page loads from is named in `privacy.html`** (the same
  test checks).

## Campaign content

- Keep factual claims about named councils, councillors and operators sourced,
  dated and fair. The evidence-provenance rules already require a method,
  `as_of` and caveats for every derived figure; follow them for anything that
  names a person or organisation. Opinion should read as opinion.
- Correct mistakes quickly and visibly. A prompt correction is the best answer
  to a complaint and the strongest position if one ever becomes a claim.
- Submissions are reviewed before publication, which is also when to remove
  other people's personal details and anything that could be defamatory.

### Elections and digital imprints

Under the Elections Act 2022 digital imprint rules, as they stood in September
2026, an **unregistered** non-party campaigner does not need an imprint on
**unpaid** material. That changes if any of these become true:

- the project pays to promote anything (ads, boosted posts),
- the project registers with the Electoral Commission as a non-party campaigner,
- the proposed extension of the rules to unregistered campaigners' organic
  material becomes law.

Before a council or general election, and before any paid promotion, re-read
the Electoral Commission's statutory guidance on digital imprints and
non-party campaigning, particularly for material that urges people to contact
or support particular councillors or candidates.

## Done on 15 September 2026

- Submissions filed to a private inbox instead of the public repository.
- A publication box on every form, checked by the Worker too.
- `privacy.html`: controller, private contact, what is collected and why,
  lawful bases, retention, recipients, US transfers, rights, ICO complaints,
  cookies and storage, and a switch to stop visit counting.
- `terms.html`: information is a guide, submission rules and licence, reuse,
  liability (without limiting what the law does not allow), England and Wales.
- Credits required by TransportAPI, the ONS, Ordnance Survey, Royal Mail and
  the Open Government Licence on `about.html`.
- OpenStreetMap tiles from the single host the tile usage policy names.
- Fonts and Leaflet served from the site instead of Google Fonts and unpkg.
- `LICENSE` (MIT, code) and `LICENSE-content.md` (CC BY 4.0, content).

## Still needs a person

- [ ] **Set up the private inbox**: create `dennislemennace/adur-worthing-bus-inbox`
      as Private, clone the labels into it, give the fine-grained token access to
      it, then deploy the Worker (steps in `worker/README.md`).
- [ ] **Set up `privacy@worthingbrightonbus.co.uk`** with Cloudflare Email Routing,
      and send a test message. The pages already publish the address.
- [ ] **Review the existing public issues** in `dennislemennace/adur-worthing-bus`
      that were filed by the old submission Worker. Delete any carrying someone's
      name, personal details or anything you would not publish today.
- [ ] **ICO data protection fee**: run the ICO's self-assessment
      (<https://ico.org.uk/for-organisations/data-protection-fee/data-protection-fee-self-assessment/>)
      and pay if it says so (tier 1 was £52 a year in 2026).
- [ ] **Photo rights**: confirm the licence for the three photographs marked
      unconfirmed in `docs/ASSET_RIGHTS.md`, or replace them.
- [ ] **Online Safety Act**: with submissions now reviewed before anything is
      public, the site is less likely to be a user-to-user service, but check with
      Ofcom's online safety regulation checker and keep the result here.
- [ ] **Optional**: have a solicitor read `terms.html` and `privacy.html` once.
