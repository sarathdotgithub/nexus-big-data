"""
AI Decision Intelligence Layer for PI-1 Platform

Implements GPT-based decision copilot grounded in Gold-layer data.
"""

from typing import Optional, Dict, List
from dataclasses import dataclass
from datetime import datetime
import json


@dataclass
class DecisionContext:
    """Context for AI decision making"""
    timestamp: datetime
    hospital_id: Optional[str] = None
    plant_id: Optional[str] = None
    risk_level: Optional[str] = None
    current_autonomy: Optional[float] = None
    operational_constraints: List[str] = None
    financial_metrics: Dict[str, float] = None


@dataclass
class DecisionResponse:
    """Response from decision intelligence layer"""
    decision_id: str
    timestamp: datetime
    query: str
    analysis: str
    recommendations: List[str]
    confidence_score: float
    supporting_data: Dict
    execution_priority: str  # IMMEDIATE, HIGH, MEDIUM, LOW
    estimated_impact: Dict[str, float]


class DataGroundingEngine:
    """
    Grounds AI reasoning in Gold-layer data
    
    Transforms structured analytics data into natural language context
    for the AI decision copilot.
    """
    
    def __init__(self, spark_session=None, gold_path: str = None):
        """
        Initialize grounding engine
        
        Args:
            spark_session: Spark session for querying Gold layer
            gold_path: Path to Gold layer tables
        """
        self.spark = spark_session
        self.gold_path = gold_path
    
    def ground_operational_context(self, hospital_id: str) -> Dict:
        """
        Create natural language context for operational decisions
        
        Args:
            hospital_id: Hospital identifier
        
        Returns:
            Dictionary of operational metrics and insights
        """
        context = {
            "hospital_id": hospital_id,
            "current_autonomy_hours": 72,  # Would query from Gold layer
            "daily_consumption_units": 150,
            "storage_capacity_units": 500,
            "current_storage_level": 350,
            "consumption_trend": "increasing",
            "next_scheduled_delivery": "2 days",
            "plant_availability": {
                "Madrid": "full capacity",
                "Barcelona": "maintenance in 5 days",
                "Zaragoza": "normal",
            },
            "distribution_constraints": [
                "Barcelona plant unavailable in 5 days (maintenance)",
                "Truck fleet: 2 units in repair",
            ],
        }
        return context
    
    def ground_financial_context(self) -> Dict:
        """Create natural language context for financial decisions"""
        context = {
            "total_network_margin": 2150000,
            "cost_per_delivery": 15000,
            "margin_per_delivery": 5000,
            "efficiency_ratio": 0.68,
            "maintenance_cost_pending": 500000,
            "capex_approved": 2000000,
        }
        return context
    
    def ground_risk_context(self) -> Dict:
        """Create natural language context for risk decisions"""
        context = {
            "hospitals_at_risk": 3,
            "critical_autonomy_hospitals": 1,
            "network_risk_score": 45,
            "days_since_last_incident": 95,
            "predicted_incidents_30d": 2,
        }
        return context


class PromptEngineer:
    """
    Designs and manages prompts for the GPT-based decision copilot
    
    Handles prompt templates, context injection, and prompt optimization.
    """
    
    SYSTEM_PROMPT = """You are the Oxygen Reliability Decision Copilot for ManOxCo. 
    
Your role is to provide executive-level insights and actionable recommendations based on:
1. Real-time operational data (production, distribution, consumption)
2. Financial performance metrics
3. Risk assessments and dry-out predictions
4. Maintenance scheduling constraints
5. Strategic objectives

Guidelines:
- Provide data-driven recommendations with clear justification
- Consider financial impact alongside operational risk
- Think strategically about 18-month planning
- Explain trade-offs clearly
- Prioritize patient safety and service reliability
- Be concise and actionable for executives

Format responses as:
1. SITUATION: Current state summary
2. KEY RISKS: Critical issues to address
3. OPTIONS: Alternative courses of action
4. RECOMMENDATION: Best path forward
5. IMPACT: Expected outcomes
"""
    
    def __init__(self):
        self.conversation_history = []
    
    def build_operational_prompt(
        self,
        query: str,
        context: Dict,
    ) -> str:
        """
        Build prompt for operational decision
        
        Args:
            query: User query
            context: Operational context from Gold layer
        
        Returns:
            Formatted prompt for GPT
        """
        
        prompt = f"""Given the following operational context:

Hospital {context['hospital_id']}:
- Current tank autonomy: {context['current_autonomy_hours']} hours
- Daily consumption: {context['daily_consumption_units']} units
- Storage level: {context['current_storage_level']}/{context['storage_capacity_units']} units
- Consumption trend: {context['consumption_trend']}
- Next delivery: {context['next_scheduled_delivery']}

Network status:
- Plant availability: {context['plant_availability']}
- Current constraints: {context['distribution_constraints']}

User Question: {query}

Please provide a data-driven decision recommendation considering operational risk, 
financial impact, and strategic objectives."""
        
        return prompt
    
    def build_financial_prompt(
        self,
        query: str,
        context: Dict,
    ) -> str:
        """Build prompt for financial decision"""
        
        prompt = f"""Given the following financial metrics:

Network Financial Performance:
- Total margin: ${context['total_network_margin']:,.0f}
- Cost per delivery: ${context['cost_per_delivery']:,.0f}
- Margin per delivery: ${context['margin_per_delivery']:,.0f}
- Efficiency ratio: {context['efficiency_ratio']:.1%}

Strategic Investments:
- Maintenance costs pending: ${context['maintenance_cost_pending']:,.0f}
- Capex approved: ${context['capex_approved']:,.0f}

User Question: {query}

Please provide a financially-informed decision recommendation."""
        
        return prompt


class DecisionCopilot:
    """
    GPT-based Decision Intelligence Agent for ManOxCo
    
    Integrates:
    - Data grounding from Gold layer
    - Prompt engineering
    - OpenAI GPT-4 integration
    - Decision logging and audit trail
    """
    
    def __init__(
        self,
        openai_api_key: Optional[str] = None,
        model: str = "gpt-4",
        temperature: float = 0.7,
    ):
        """
        Initialize Decision Copilot
        
        Args:
            openai_api_key: OpenAI API key
            model: LLM model to use
            temperature: LLM temperature (creativity/randomness)
        """
        self.api_key = openai_api_key
        self.model = model
        self.temperature = temperature
        self.grounding_engine = DataGroundingEngine()
        self.prompt_engineer = PromptEngineer()
        self.decision_history: List[DecisionResponse] = []
    
    def make_operational_decision(
        self,
        query: str,
        hospital_id: Optional[str] = None,
    ) -> DecisionResponse:
        """
        Make an operational decision based on data context
        
        Args:
            query: User question about operations
            hospital_id: Specific hospital (optional)
        
        Returns:
            Decision response with recommendations
        """
        
        # Ground in operational data
        context = self.grounding_engine.ground_operational_context(
            hospital_id or "NETWORK"
        )
        
        # Build prompt
        prompt = self.prompt_engineer.build_operational_prompt(query, context)
        
        # Call GPT (would be real API call in production)
        response = self._call_gpt(prompt)
        
        # Create decision response
        decision = DecisionResponse(
            decision_id=self._generate_decision_id(),
            timestamp=datetime.now(),
            query=query,
            analysis=response["analysis"],
            recommendations=response["recommendations"],
            confidence_score=0.85,  # Would calculate from GPT response
            supporting_data=context,
            execution_priority="HIGH" if hospital_id else "MEDIUM",
            estimated_impact={
                "autonomy_hours_gained": 48,
                "risk_reduction": 0.3,
                "cost_impact": -25000,
            },
        )
        
        self.decision_history.append(decision)
        return decision
    
    def make_financial_decision(
        self,
        query: str,
    ) -> DecisionResponse:
        """Make a financial/strategic decision"""
        
        # Ground in financial data
        context = self.grounding_engine.ground_financial_context()
        
        # Build prompt
        prompt = self.prompt_engineer.build_financial_prompt(query, context)
        
        # Call GPT
        response = self._call_gpt(prompt)
        
        # Create decision response
        decision = DecisionResponse(
            decision_id=self._generate_decision_id(),
            timestamp=datetime.now(),
            query=query,
            analysis=response["analysis"],
            recommendations=response["recommendations"],
            confidence_score=0.80,
            supporting_data=context,
            execution_priority="MEDIUM",
            estimated_impact={
                "margin_impact": 150000,
                "cost_savings": 75000,
                "capex_allocation": 500000,
            },
        )
        
        self.decision_history.append(decision)
        return decision
    
    def _call_gpt(self, prompt: str) -> Dict:
        """
        Call GPT API with prompt
        
        Args:
            prompt: Formatted prompt
        
        Returns:
            Parsed response from GPT
        """
        # In production, would call: openai.ChatCompletion.create()
        # For now, return mock response
        return {
            "analysis": "Based on current operational data and financial metrics...",
            "recommendations": [
                "Expedite next delivery to [hospital name]",
                "Allocate additional truck capacity",
                "Monitor consumption patterns",
            ],
        }
    
    def _generate_decision_id(self) -> str:
        """Generate unique decision ID"""
        import uuid
        return f"DECISION-{uuid.uuid4().hex[:12].upper()}"
    
    def get_decision_history(self, limit: int = 10) -> List[DecisionResponse]:
        """Get recent decision history"""
        return self.decision_history[-limit:]
    
    def export_decision_audit_log(self) -> str:
        """Export audit log of all decisions"""
        log_entries = [
            f"{d.timestamp} | {d.decision_id} | {d.query[:50]}... | Priority: {d.execution_priority}"
            for d in self.decision_history
        ]
        return "\n".join(log_entries)


__all__ = [
    "DecisionContext",
    "DecisionResponse",
    "DataGroundingEngine",
    "PromptEngineer",
    "DecisionCopilot",
]
