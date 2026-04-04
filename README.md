# Cacheconomy – Team Apex

## Overview
Cacheconomy is a data-driven initiative focused on uncovering and organizing hidden economic value within local communities. This project aims to transform fragmented and outdated business data into a structured, reliable dataset that better represents the true landscape of small businesses.

Team Apex contributes by working on data analysis, organization, and validation to support the creation of a high-quality business index.

---

## Objectives
- Improve the accuracy of small business data  
- Identify and reduce inconsistencies or outdated records  
- Support the creation of a unified and reliable dataset  
- Contribute to insights that help better understand local economic activity  

---

## Our Role
Team Apex focuses on:
- Reviewing and organizing large datasets  
- Identifying patterns and inconsistencies in data  
- Supporting data validation and quality improvement  
- Collaborating as a team to ensure efficient progress  

---

## Tools & Technologies
- Python  
- Pandas  
- SQL  
- Supabase  

---

## Skills Applied
- Data Analysis  
- Problem Solving  
- Team Collaboration  
- Pattern Recognition  
- Data Validation  

---

## Project Context
This work is part of a university group internship course in collaboration with Advocations. The project emphasizes hands-on learning and real-world application of data skills in a collaborative environment.

---

## Team Apex
- Team-based collaboration focused on delivering meaningful data insights  
- Emphasis on organization, consistency, and continuous improvement  

---

## Notes
This repository contains work related to the Cacheconomy project. Some datasets and details may be restricted due to project scope and data privacy considerations.

---

## Environment Variables (.env)

Create a `.env` file in the project root with the following variables:

```
SUPABASE_URL=your_supabase_project_url
SUPABASE_SERVICE_ROLE_KEY=your_supabase_service_role_secret
SUPABASE_TABLE=your_table_name
SUPABASE_PAGE_SIZE=1000
SUPABASE_MAX_PAGES=3
```

- `SUPABASE_URL`: Your Supabase project URL.
- `SUPABASE_SERVICE_ROLE_KEY`: Service role secret key (for admin/server-side scripts only).
- `SUPABASE_TABLE`: The table to query.
- `SUPABASE_PAGE_SIZE`: Number of rows to fetch per page (for pagination).
- `SUPABASE_MAX_PAGES`: Maximum number of pages to fetch in one run.

**Never commit your .env file or secret keys to version control.**