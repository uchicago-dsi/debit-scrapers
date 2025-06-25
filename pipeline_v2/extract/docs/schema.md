# Schema

- bank
- number
- name
- status
- under_appraisal_utc
- approved_utc
- signed_utc
- disclosed_utc
- effective_utc
- closed_original_utc
- closed_revised_utc
- closed_actual_utc
- started_utc
- planned_completed_utc
- completed_utc
- last_updated_utc
- type
- loan_amount
- loan_amount_currency
- loan_amount_usd
- sectors
- countries
- companies
- url

\*\* Add finance type

- [ ] Correct schema
- [ ] Handles/surfaces errors correctly
- [ ] Handles imports consistetly
- [ ] No redundant constructors
- [ ] List items joined by pipe delimiter

Needs an LLM:

- AIIB for companies (i.e., borrower and implementing entity)
- EBRD for companies (i.e., client information)
- FMO for companies
- KFW for translation
- MIGA for companies (full list, not just guarantee holder)

## Next up:

- Add finance type to all scrapers
- Document scrapers and wrap up cleaning (standard schema)
- Update models and DAL to reflect new fields
- Test DB logic
