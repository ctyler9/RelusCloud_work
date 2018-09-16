SELECT opportunity.closedate,
        account.industry AS Market_Segment,
        campaign_member.name AS Solution_Architect,
        sow_request.sow_number__c
FROM opportunity
JOIN account
   ON opportunity.accountid = account.id
JOIN campaign_member
   ON opportunity.campaignid = campaign_member.campaignid
JOIN sow_request
   ON opportunity.createdbyid = sow_request.createdbyid
