{{ config(materialized='table') }}

with departmental_financial_data as (
    select
        d.department_id,
        d.department_name,
        d.department_code,
        d.budget as allocated_budget,
        d.budget_millions,
        d.department_size,
        count(distinct f.faculty_id) as faculty_count,
        count(distinct s.student_id) as student_count,
        count(distinct c.course_id) as course_offerings,
        sum(f.salary) as total_faculty_salaries,
        avg(f.salary) as avg_faculty_salary,
        sum(tp.amount) as total_tuition_revenue,
        sum(fa.amount) as total_aid_disbursed,
        count(distinct e.enrollment_id) as total_enrollments,
        avg(e.grade_points) as dept_avg_gpa,
        count(case when s.student_status = 'graduated' then 1 end) as graduates_produced,
        round(avg(e.attendance_percentage), 2) as dept_avg_attendance
    from {{ ref('stg_departments') }} d
    left join {{ ref('stg_faculty') }} f on d.department_id = f.department_id
    left join {{ ref('stg_courses') }} c on d.department_id = c.department_id
    left join {{ ref('stg_students') }} s on d.department_id = s.major_id
    left join {{ ref('stg_enrollments') }} e on c.course_id = e.course_id and s.student_id = e.student_id
    left join {{ ref('stg_tuition_payments') }} tp on s.student_id = tp.student_id
    left join {{ ref('stg_financial_aid') }} fa on s.student_id = fa.student_id
    group by 
        d.department_id, d.department_name, d.department_code, d.budget, 
        d.budget_millions, d.department_size
),

cost_benefit_analysis as (
    select
        dfd.*,
        -- Cost efficiency metrics
        round(allocated_budget / nullif(student_count, 0), 2) as cost_per_student,
        round(allocated_budget / nullif(faculty_count, 0), 2) as cost_per_faculty,
        round(allocated_budget / nullif(graduates_produced, 0), 2) as cost_per_graduate,
        round(allocated_budget / nullif(total_enrollments, 0), 2) as cost_per_enrollment,
        
        -- Revenue efficiency metrics
        round(total_tuition_revenue / nullif(allocated_budget, 0), 2) as revenue_to_budget_ratio,
        round(total_tuition_revenue / nullif(student_count, 0), 2) as revenue_per_student,
        round((total_tuition_revenue - total_aid_disbursed) / nullif(allocated_budget, 0), 2) as net_revenue_ratio,
        
        -- Academic output metrics
        round(graduates_produced / nullif(allocated_budget, 0) * 100000, 2) as graduates_per_100k_budget,
        round(dept_avg_gpa * total_enrollments / nullif(allocated_budget, 0) * 10000, 2) as quality_weighted_output,
        
        -- Resource utilization metrics
        round(total_faculty_salaries / nullif(allocated_budget, 0) * 100, 2) as faculty_cost_percentage,
        round(student_count / nullif(faculty_count, 0), 2) as student_faculty_ratio,
        round(total_enrollments / nullif(course_offerings, 0), 2) as avg_class_size
    from departmental_financial_data dfd
),

performance_benchmarking as (
    select
        cba.*,
        -- Percentile rankings for benchmarking
        percent_rank() over (order by revenue_to_budget_ratio) as revenue_efficiency_percentile,
        percent_rank() over (order by cost_per_graduate) as cost_effectiveness_percentile,
        percent_rank() over (order by quality_weighted_output desc) as quality_output_percentile,
        percent_rank() over (order by graduates_per_100k_budget desc) as graduate_productivity_percentile,
        
        -- Comparative analysis
        avg(cost_per_student) over () as institutional_avg_cost_per_student,
        avg(revenue_to_budget_ratio) over () as institutional_avg_revenue_ratio,
        avg(dept_avg_gpa) over () as institutional_avg_gpa,
        avg(student_faculty_ratio) over () as institutional_avg_ratio,
        
        -- Performance categories
        case
            when revenue_to_budget_ratio >= 1.5 then 'High Revenue Generator'
            when revenue_to_budget_ratio >= 1.2 then 'Good Revenue Generator'
            when revenue_to_budget_ratio >= 1.0 then 'Break-Even'
            when revenue_to_budget_ratio >= 0.8 then 'Moderate Loss'
            else 'High Loss'
        end as revenue_performance_category,
        
        case
            when cost_per_graduate <= 50000 then 'Highly Cost Effective'
            when cost_per_graduate <= 100000 then 'Cost Effective'
            when cost_per_graduate <= 200000 then 'Moderately Cost Effective'
            else 'Costly'
        end as cost_effectiveness_category,
        
        case
            when quality_weighted_output >= 50 then 'High Quality Output'
            when quality_weighted_output >= 30 then 'Good Quality Output'
            when quality_weighted_output >= 20 then 'Adequate Quality Output'
            else 'Low Quality Output'
        end as quality_output_category
    from cost_benefit_analysis cba
),

optimization_opportunities as (
    select
        pb.*,
        -- Budget optimization score (0-100)
        round(
            (case when revenue_efficiency_percentile >= 0.8 then 25
                  when revenue_efficiency_percentile >= 0.6 then 20
                  when revenue_efficiency_percentile >= 0.4 then 15
                  else 10 end) +
            (case when cost_effectiveness_percentile >= 0.8 then 25
                  when cost_effectiveness_percentile >= 0.6 then 20
                  when cost_effectiveness_percentile >= 0.4 then 15
                  else 10 end) +
            (case when quality_output_percentile >= 0.8 then 25
                  when quality_output_percentile >= 0.6 then 20
                  when quality_output_percentile >= 0.4 then 15
                  else 10 end) +
            (case when graduate_productivity_percentile >= 0.8 then 25
                  when graduate_productivity_percentile >= 0.6 then 20
                  when graduate_productivity_percentile >= 0.4 then 15
                  else 10 end), 0
        ) as budget_optimization_score,
        
        -- Specific optimization recommendations
        case
            when revenue_to_budget_ratio < 0.8 and student_faculty_ratio < 15 then 'Increase class sizes or reduce faculty'
            when revenue_to_budget_ratio < 0.8 and course_offerings > student_count * 0.8 then 'Consolidate course offerings'
            when cost_per_graduate > 150000 and dept_avg_gpa < 3.0 then 'Improve academic support for better retention'
            when faculty_cost_percentage > 80 then 'Review faculty compensation structure'
            when student_count < 100 and allocated_budget > 1000000 then 'Consider program consolidation or growth'
            when revenue_to_budget_ratio > 1.5 and quality_weighted_output > 50 then 'Model department - consider expansion'
            else 'Minor optimizations recommended'
        end as primary_optimization_recommendation,
        
        -- Budget reallocation suggestions
        case
            when revenue_performance_category = 'High Revenue Generator' and cost_effectiveness_category = 'Highly Cost Effective' then
                'Increase budget allocation for expansion'
            when revenue_performance_category in ('Moderate Loss', 'High Loss') and cost_effectiveness_category = 'Costly' then
                'Reduce budget allocation and restructure'
            when quality_output_category = 'Low Quality Output' then
                'Reallocate funds to academic support and faculty development'
            when student_faculty_ratio > institutional_avg_ratio * 1.5 then
                'Allocate additional faculty positions'
            else 'Maintain current allocation with efficiency improvements'
        end as budget_reallocation_suggestion
    from performance_benchmarking pb
),

budget_adjustments as (
    select
        oo.*,
        -- Estimated budget adjustment
        case
            when oo.budget_optimization_score >= 80 then round(oo.allocated_budget * 1.1, 0)  -- 10% increase
            when oo.budget_optimization_score >= 60 then oo.allocated_budget  -- No change
            when oo.budget_optimization_score >= 40 then round(oo.allocated_budget * 0.95, 0)  -- 5% decrease
            else round(oo.allocated_budget * 0.85, 0)  -- 15% decrease
        end as suggested_budget_allocation
    from optimization_opportunities oo
),

portfolio_optimization as (
    select
        ba.*,
        ba.suggested_budget_allocation - ba.allocated_budget as budget_change_amount,
        round(
            (ba.suggested_budget_allocation - ba.allocated_budget) / nullif(ba.allocated_budget, 0) * 100, 2
        ) as budget_change_percentage,
        
        -- Portfolio impact analysis
        sum(ba.suggested_budget_allocation) over () as total_suggested_budget,
        sum(ba.allocated_budget) over () as total_current_budget,
        round(
            (sum(ba.suggested_budget_allocation) over () - sum(ba.allocated_budget) over ()) /
            nullif(sum(ba.allocated_budget) over (), 0) * 100, 2
        ) as institutional_budget_change_percentage,

        -- ROI projections
        case
            when (ba.suggested_budget_allocation - ba.allocated_budget) / nullif(ba.allocated_budget, 0) * 100 > 0 then
                round(ba.graduates_per_100k_budget * 1.1, 2)  -- Projected 10% improvement
            when (ba.suggested_budget_allocation - ba.allocated_budget) / nullif(ba.allocated_budget, 0) * 100 = 0 then
                ba.graduates_per_100k_budget
            else
                round(ba.graduates_per_100k_budget * 0.9, 2)  -- Potential 10% decline
        end as projected_graduate_productivity,

        case
            when (ba.suggested_budget_allocation - ba.allocated_budget) / nullif(ba.allocated_budget, 0) * 100 > 0 then
                round(ba.revenue_to_budget_ratio * 1.05, 2)  -- Projected 5% improvement
            when (ba.suggested_budget_allocation - ba.allocated_budget) / nullif(ba.allocated_budget, 0) * 100 = 0 then
                ba.revenue_to_budget_ratio
            else
                round(ba.revenue_to_budget_ratio * 1.02, 2)  -- Small improvement from efficiency
        end as projected_revenue_ratio,

        -- Strategic priority classification
        case
            when ba.revenue_performance_category = 'High Revenue Generator' and ba.quality_output_category = 'High Quality Output' then 'Strategic Growth Investment'
            when ba.revenue_performance_category in ('Moderate Loss', 'High Loss') and ba.cost_effectiveness_category = 'Costly' then 'Restructuring Priority'
            when ba.quality_output_category = 'Low Quality Output' and ba.student_count > 200 then 'Quality Improvement Priority'
            when ba.student_count < 50 and ba.revenue_performance_category != 'High Revenue Generator' then 'Viability Assessment Required'
            else 'Efficiency Optimization'
        end as strategic_priority
    from budget_adjustments ba
)

select * from portfolio_optimization
order by budget_optimization_score desc, strategic_priority